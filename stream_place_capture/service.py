from __future__ import annotations

import asyncio
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp  # type: ignore[import-not-found]

from .api import resolve_handle
from .config import CaptureConfig, StreamTarget
from .remux import Remuxer
from .segment_capture import SegmentSubscriber, SegmentWriter
from .state import StateStore


def setup_logging(log_root: Path) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_root / "capture.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


@dataclass
class RunningWorker:
    target: StreamTarget
    task: asyncio.Task[None]
    stop_event: asyncio.Event


class CaptureService:
    def __init__(self, cfg: CaptureConfig) -> None:
        self.cfg = cfg
        self.log = logging.getLogger("capture-service")
        self.stop_event = asyncio.Event()
        self.workers: dict[str, RunningWorker] = {}
        self.resolved_targets: list[StreamTarget] = []
        self.state = StateStore(cfg.log_root / "state.json")
        self.remuxers_by_stream: dict[str, Remuxer] = {}

    async def _resolve_targets(self, session: aiohttp.ClientSession) -> None:
        self.resolved_targets = []
        for t in self.cfg.stream_targets:
            try:
                resolved = await resolve_handle(session, self.cfg.endpoint, t.handle)
                if resolved != t.did:
                    self.log.warning(
                        "handle DID changed for @%s configured=%s resolved=%s (using resolved)",
                        t.handle,
                        t.did,
                        resolved,
                    )
                self.resolved_targets.append(StreamTarget(name=t.name, handle=t.handle, did=resolved))
            except Exception as exc:
                self.log.error("failed to resolve @%s, using configured did %s (%s)", t.handle, t.did, exc)
                self.resolved_targets.append(t)

    async def _run_worker(self, target: StreamTarget, stop_event: asyncio.Event, session: aiohttp.ClientSession) -> None:
        stream_segment_dir = self.cfg.capture_root / target.name / "segments"
        quality_dir = self.cfg.capture_root / target.name / "quality"

        def on_segment(result) -> None:
            self.state.on_segment(target.name, result.epoch_ms, result.bytes_written)

        writer = SegmentWriter(
            stream_name=target.name,
            stream_dir=stream_segment_dir,
            quality_dir=quality_dir,
            require_1080p30=self.cfg.require_1080p30,
            on_segment=on_segment,
        )
        remuxer = Remuxer(
            ffmpeg_path=self.cfg.ffmpeg_path,
            stream_name=target.name,
            segment_dir=stream_segment_dir,
            output_dir=self.cfg.final_root / target.name,
        )
        self.remuxers_by_stream[target.name] = remuxer
        subscriber = SegmentSubscriber(
            endpoint=self.cfg.endpoint,
            streamer_did=target.did,
            stream_name=target.name,
            writer=writer,
            ping_interval_seconds=self.cfg.ping_interval_seconds,
            idle_timeout_seconds=self.cfg.websocket_idle_timeout_seconds,
        )

        total_segments = 0
        last_remux_ts = 0.0
        reconnect_delay = self.cfg.reconnect_delay_seconds
        try:
            while not stop_event.is_set() and not self.stop_event.is_set():
                try:
                    self.state.on_connected(target.name)
                    count = await subscriber.run_once(session)
                    total_segments += count
                    reconnect_delay = self.cfg.reconnect_delay_seconds
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log.warning("worker %s disconnected: %s", target.name, exc)
                    self.state.on_disconnect(target.name, str(exc))

                if self.cfg.enable_realtime_remux:
                    now = time.monotonic()
                    should_remux = total_segments >= self.cfg.remux_every_segments
                    if not should_remux and self.cfg.remux_interval_seconds > 0:
                        should_remux = (now - last_remux_ts) >= self.cfg.remux_interval_seconds
                    if should_remux:
                        remuxer.remux_progressive()
                        total_segments = 0
                        last_remux_ts = now

                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max(self.cfg.reconnect_delay_seconds, reconnect_delay * 2), self.cfg.max_reconnect_delay_seconds)
        finally:
            if self.cfg.enable_realtime_remux:
                remuxer.remux_progressive()
            self.state.on_stopped(target.name)
            self.remuxers_by_stream.pop(target.name, None)

    async def _start_all_workers(self, session: aiohttp.ClientSession) -> None:
        for target in self.resolved_targets:
            self.state.init_stream(target.name, target.handle, target.did)
            stop_event = asyncio.Event()
            task = asyncio.create_task(self._run_worker(target, stop_event, session), name=f"capture-{target.name}")
            self.workers[target.did] = RunningWorker(target=target, task=task, stop_event=stop_event)
            self.log.info("started capture worker for %s (@%s)", target.name, target.handle)

    async def _monitor_workers(self, session: aiohttp.ClientSession) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            for did, worker in list(self.workers.items()):
                if not worker.task.done():
                    continue
                if worker.task.cancelled():
                    self.log.info("worker %s cancelled", worker.target.name)
                else:
                    exc = worker.task.exception()
                    if exc:
                        self.log.warning("worker %s exited with error: %s", worker.target.name, exc)
                    else:
                        self.log.warning("worker %s exited unexpectedly, restarting", worker.target.name)

                if self.stop_event.is_set():
                    break

                stop_event = asyncio.Event()
                task = asyncio.create_task(
                    self._run_worker(worker.target, stop_event, session),
                    name=f"capture-{worker.target.name}",
                )
                self.workers[did] = RunningWorker(target=worker.target, task=task, stop_event=stop_event)
                self.log.info("restarted capture worker for %s", worker.target.name)

    async def run(self) -> None:
        headers = {}
        if self.cfg.access_jwt:
            headers["Authorization"] = f"Bearer {self.cfg.access_jwt}"

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            await self._resolve_targets(session)
            self.log.info("tracking %d streams", len(self.resolved_targets))

            await self._start_all_workers(session)

            monitor_task = asyncio.create_task(self._monitor_workers(session), name="worker-monitor")
            await self.stop_event.wait()
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

            for running in self.workers.values():
                running.stop_event.set()
                running.task.cancel()
            await asyncio.gather(*(w.task for w in self.workers.values()), return_exceptions=True)
            self.workers.clear()

    async def checkpoint_and_stop(self) -> None:
        self.log.info("checkpoint stop requested")
        for remuxer in list(self.remuxers_by_stream.values()):
            try:
                remuxer.remux_progressive()
                remuxer.archive_live_output()
            except Exception as exc:
                self.log.warning("checkpoint remux/archive failed: %s", exc)
        self.request_stop()

    def request_stop(self) -> None:
        self.stop_event.set()


async def run_service(cfg: CaptureConfig) -> None:
    setup_logging(cfg.log_root)
    service = CaptureService(cfg)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, service.request_stop)
        except NotImplementedError:
            pass

    await service.run()
