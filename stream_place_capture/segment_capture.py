from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import aiohttp  # type: ignore[import-not-found]

from .mp4_safety import is_mp4_segment, is_roughly_1080p30, parse_video_info


@dataclass
class SegmentResult:
    segment_path: Path
    bytes_written: int
    epoch_ms: int


class SegmentWriter:
    def __init__(
        self,
        stream_name: str,
        stream_dir: Path,
        quality_dir: Path,
        require_1080p30: bool,
        on_segment: Callable[[SegmentResult], None] | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.stream_dir = stream_dir
        self.quality_dir = quality_dir
        self.require_1080p30 = require_1080p30
        self.on_segment = on_segment
        self.stream_dir.mkdir(parents=True, exist_ok=True)
        self.quality_dir.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(f"segment-writer.{stream_name}")
        self._seq = 0

    def write_segment(self, payload: bytes) -> SegmentResult | None:
        if not payload:
            return None
        if not is_mp4_segment(payload):
            self.log.warning("received non-mp4 websocket payload len=%d", len(payload))
            return None

        info = parse_video_info(payload)
        is_1080p30 = is_roughly_1080p30(info)
        if self.require_1080p30 and not is_1080p30:
            self.log.warning(
                "segment rejected by quality gate width=%s height=%s fps=%s",
                info.width,
                info.height,
                None if info.frame_rate is None else round(info.frame_rate, 3),
            )
            return None

        epoch_ms = int(time.time() * 1000)
        self._seq += 1
        file_id = f"{epoch_ms}-{self._seq:08d}"
        temp_path = self.stream_dir / f"{file_id}.mp4.tmp"
        final_path = self.stream_dir / f"{file_id}.mp4"

        with temp_path.open("wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        temp_path.replace(final_path)

        metadata = {
            "epoch_ms": epoch_ms,
            "bytes": len(payload),
            "width": info.width,
            "height": info.height,
            "fps": info.frame_rate,
            "roughly_1080p30": is_1080p30,
            "file": final_path.name,
        }
        (self.quality_dir / f"{file_id}.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        result = SegmentResult(segment_path=final_path, bytes_written=len(payload), epoch_ms=epoch_ms)
        if self.on_segment:
            self.on_segment(result)
        return result


class SegmentSubscriber:
    def __init__(
        self,
        endpoint: str,
        streamer_did: str,
        stream_name: str,
        writer: SegmentWriter,
        ping_interval_seconds: int,
        idle_timeout_seconds: int,
    ) -> None:
        self.endpoint = endpoint
        self.streamer_did = streamer_did
        self.stream_name = stream_name
        self.writer = writer
        self.ping_interval_seconds = ping_interval_seconds
        self.idle_timeout_seconds = idle_timeout_seconds
        self.log = logging.getLogger(f"subscriber.{stream_name}")

    async def run_once(self, session: aiohttp.ClientSession) -> int:
        ws_url = self.endpoint.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/xrpc/place.stream.live.subscribeSegments?streamer={self.streamer_did}"

        segment_count = 0
        last_message = time.monotonic()

        self.log.info("connecting websocket %s", ws_url)
        async with session.ws_connect(
            ws_url,
            heartbeat=self.ping_interval_seconds,
            autoping=True,
            autoclose=True,
            max_msg_size=40 * 1024 * 1024,
        ) as ws:
            self.log.info("websocket connected")
            while True:
                timeout = self.idle_timeout_seconds - (time.monotonic() - last_message)
                if timeout <= 0:
                    raise TimeoutError("websocket idle timeout")

                msg = await ws.receive(timeout=timeout)
                if msg.type == aiohttp.WSMsgType.BINARY:
                    last_message = time.monotonic()
                    result = self.writer.write_segment(msg.data)
                    if result:
                        segment_count += 1
                        self.log.info(
                            "saved segment file=%s bytes=%d total=%d",
                            result.segment_path.name,
                            result.bytes_written,
                            segment_count,
                        )
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    last_message = time.monotonic()
                    self.log.debug("text message from server: %s", msg.data[:180])
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    self.log.warning("websocket closed by server")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"websocket error: {ws.exception()}")
        return segment_count
