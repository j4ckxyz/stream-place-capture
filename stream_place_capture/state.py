from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StreamState:
    stream_name: str
    handle: str
    did: str
    status: str
    segments: int
    bytes_total: int
    last_segment_epoch_ms: int | None
    last_error: str | None
    reconnects: int
    started_epoch_ms: int


class StateStore:
    def __init__(self, state_file: Path) -> None:
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._streams: dict[str, StreamState] = {}
        self._log = logging.getLogger("state-store")

    def init_stream(self, stream_name: str, handle: str, did: str) -> None:
        now_ms = int(time.time() * 1000)
        with self._lock:
            self._streams[stream_name] = StreamState(
                stream_name=stream_name,
                handle=handle,
                did=did,
                status="starting",
                segments=0,
                bytes_total=0,
                last_segment_epoch_ms=None,
                last_error=None,
                reconnects=0,
                started_epoch_ms=now_ms,
            )
            self._persist_locked()

    def on_connected(self, stream_name: str) -> None:
        with self._lock:
            st = self._streams[stream_name]
            st.status = "connected"
            st.last_error = None
            self._persist_locked()

    def on_segment(self, stream_name: str, epoch_ms: int, bytes_written: int) -> None:
        with self._lock:
            st = self._streams[stream_name]
            st.status = "recording"
            st.segments += 1
            st.bytes_total += bytes_written
            st.last_segment_epoch_ms = epoch_ms
            self._persist_locked()

    def on_disconnect(self, stream_name: str, error: str | None) -> None:
        with self._lock:
            st = self._streams[stream_name]
            st.status = "reconnecting"
            if error:
                st.last_error = error[:500]
            st.reconnects += 1
            self._persist_locked()

    def on_stopped(self, stream_name: str) -> None:
        with self._lock:
            st = self._streams[stream_name]
            st.status = "stopped"
            self._persist_locked()

    def _persist_locked(self) -> None:
        payload = {
            "updated_epoch_ms": int(time.time() * 1000),
            "streams": [
                {
                    "stream_name": st.stream_name,
                    "handle": st.handle,
                    "did": st.did,
                    "status": st.status,
                    "segments": st.segments,
                    "bytes_total": st.bytes_total,
                    "last_segment_epoch_ms": st.last_segment_epoch_ms,
                    "last_error": st.last_error,
                    "reconnects": st.reconnects,
                    "started_epoch_ms": st.started_epoch_ms,
                }
                for st in self._streams.values()
            ],
        }
        tmp = self.state_file.with_suffix(".tmp")
        body = json.dumps(payload, indent=2)

        # Best-effort persistence only; capture must never fail due to state I/O.
        for _ in range(4):
            try:
                tmp.write_text(body, encoding="utf-8")
                tmp.replace(self.state_file)
                return
            except PermissionError:
                time.sleep(0.05)
            except Exception as exc:
                self._log.warning("state write retry due to error: %s", exc)
                time.sleep(0.05)

        try:
            # Fallback non-atomic write if file-replace is locked by another process.
            self.state_file.write_text(body, encoding="utf-8")
        except Exception as exc:
            self._log.warning("state write skipped due to lock/error: %s", exc)
