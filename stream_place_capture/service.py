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
        segments_since_remux = 0
        last_remux_ts = time.monotonic()
        last_segment_ts = time.monotonic()

        def on_segment(result) -> None:
            nonlocal segments_since_remux, last_segment_ts
            segments_since_remux += 1
            last_segment_ts = time.monotonic()
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
            quality_preset=self.cfg.quality_preset,
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

        reconnect_delay = self.cfg.reconnect_delay_seconds

        async def periodic_remux() -> None:
            nonlocal segments_since_remux, last_remux_ts
            if not self.cfg.enable_realtime_remux:
                return
            while not stop_event.is_set() and not self.stop_event.is_set():
                await asyncio.sleep(3)
                now = time.monotonic()
                due_count = segments_since_remux >= self.cfg.remux_every_segments
                due_time = self.cfg.remux_interval_seconds > 0 and (now - last_remux_ts) >= self.cfg.remux_interval_seconds
                idle_flush = segments_since_remux > 0 and (now - last_segment_ts) >= 25
                if not (due_count or due_time or idle_flush):
                    continue

                if idle_flush:
                    out = await asyncio.to_thread(remuxer.force_full_rebuild)
                else:
                    out = await asyncio.to_thread(remuxer.remux_progressive)
                if out is None:
                    out = await asyncio.to_thread(remuxer.force_full_rebuild)

                if out is not None:
                    if self.cfg.prune_processed_segments:
                        removed = await asyncio.to_thread(
                            remuxer.prune_processed_raw_segments,
                            self.cfg.keep_recent_raw_segments,
                        )
                        if removed > 0:
                            self.log.info("pruned %d processed raw segments for %s", removed, target.name)
                    segments_since_remux = 0
                    last_remux_ts = time.monotonic()

        remux_task = asyncio.create_task(periodic_remux(), name=f"remux-{target.name}")

        try:
            while not stop_event.is_set() and not self.stop_event.is_set():
                try:
                    self.state.on_connected(target.name)
                    await subscriber.run_once(session)
                    reconnect_delay = self.cfg.reconnect_delay_seconds
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log.warning("worker %s disconnected: %s", target.name, exc)
                    self.state.on_disconnect(target.name, str(exc))

                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max(self.cfg.reconnect_delay_seconds, reconnect_delay * 2), self.cfg.max_reconnect_delay_seconds)
        finally:
            remux_task.cancel()
            try:
                await remux_task
            except asyncio.CancelledError:
                pass
            if self.cfg.enable_realtime_remux:
                out = await asyncio.to_thread(remuxer.remux_progressive)
                if out is None:
                    out = await asyncio.to_thread(remuxer.force_full_rebuild)
                if self.cfg.prune_processed_segments and out is not None:
                    await asyncio.to_thread(remuxer.prune_processed_raw_segments, self.cfg.keep_recent_raw_segments)
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
                out = remuxer.remux_progressive()
                if out is None:
                    out = remuxer.force_full_rebuild()
                remuxer.archive_live_output()
            except Exception as exc:
                self.log.warning("checkpoint remux/archive failed: %s", exc)
        self.request_stop()

    async def graceful_stop(self) -> dict[str, list[Path]]:
        self.log.info("graceful stop requested")
        outputs: dict[str, list[Path]] = {}
        for stream_name, remuxer in list(self.remuxers_by_stream.items()):
            stream_outputs: list[Path] = []
            try:
                final_path = remuxer.remux_progressive()
                if final_path is None:
                    final_path = remuxer.force_full_rebuild()
                if final_path is not None:
                    stream_outputs.append(final_path)
                checkpoint = remuxer.archive_live_output()
                if checkpoint is not None:
                    stream_outputs.append(checkpoint)
            except Exception as exc:
                self.log.warning("graceful stop remux/archive failed stream=%s err=%s", stream_name, exc)
            outputs[stream_name] = stream_outputs
        self.request_stop()
        return outputs

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
