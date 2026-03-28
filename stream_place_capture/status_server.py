from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .config import load_config


def _human_duration(ms: int) -> str:
    ms = max(0, ms)
    sec_total, rem_ms = divmod(ms, 1000)
    days, rem = divmod(sec_total, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    return f"{days}d {hours}h {mins}m {secs}s {rem_ms}ms"


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        if not p.is_file():
            continue
        try:
            total += p.stat().st_size
        except Exception:
            continue
    return total


def _count_files(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob(pattern))


def _service_is_active(name: str) -> tuple[bool, str]:
    try:
        proc = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True, timeout=3)
        state = proc.stdout.strip() or proc.stderr.strip() or "unknown"
        return (proc.returncode == 0), state
    except Exception as exc:
        return False, f"error:{exc}"


@dataclass
class StatusContext:
    config_path: Path
    started_epoch_ms: int
    cache_ttl_seconds: int = 5
    last_payload_epoch_ms: int = 0
    last_payload: dict[str, Any] | None = None

    def build_payload(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        if self.last_payload and (now_ms - self.last_payload_epoch_ms) < (self.cache_ttl_seconds * 1000):
            return self.last_payload

        cfg = load_config(self.config_path)
        root = self.config_path.parent.parent.resolve()

        capture_root = (root / cfg.capture_root).resolve() if not cfg.capture_root.is_absolute() else cfg.capture_root.resolve()
        final_root = (root / cfg.final_root).resolve() if not cfg.final_root.is_absolute() else cfg.final_root.resolve()
        log_root = (root / cfg.log_root).resolve() if not cfg.log_root.is_absolute() else cfg.log_root.resolve()

        state_path = log_root / "state.json"
        state_data: dict[str, Any] = {}
        if state_path.exists():
            try:
                state_data = json.loads(state_path.read_text(encoding="utf-8"))
            except Exception:
                state_data = {}

        streams = []
        started_values: list[int] = []
        for st in state_data.get("streams", []) or []:
            started_raw = st.get("started_epoch_ms")
            started_ms = int(started_raw) if isinstance(started_raw, int) else None
            if started_ms is not None:
                started_values.append(started_ms)

            last_raw = st.get("last_segment_epoch_ms")
            last_age_seconds = None
            if isinstance(last_raw, int):
                last_age_seconds = max(0, (now_ms - last_raw) // 1000)

            streams.append(
                {
                    "stream_name": st.get("stream_name"),
                    "handle": st.get("handle"),
                    "did": st.get("did"),
                    "status": st.get("status"),
                    "segments": int(st.get("segments") or 0),
                    "bytes_total": int(st.get("bytes_total") or 0),
                    "last_segment_age_seconds": last_age_seconds,
                    "reconnects": int(st.get("reconnects") or 0),
                    "last_error": st.get("last_error"),
                    "started_epoch_ms": started_ms,
                }
            )

        recording_started_ms = min(started_values) if started_values else None
        recording_duration_ms = (now_ms - recording_started_ms) if recording_started_ms else 0

        capture_bytes = _dir_size_bytes(capture_root)
        final_bytes = _dir_size_bytes(final_root)
        log_bytes = _dir_size_bytes(log_root)
        total_saved_bytes = capture_bytes + final_bytes + log_bytes

        final_live_outputs = _count_files(final_root, "*.live.mp4")
        final_checkpoint_outputs = _count_files(final_root, "*.checkpoint.mp4")
        final_chunk_outputs = _count_files(final_root, "*.chunk.*.mp4")

        usage_target = capture_root if capture_root.exists() else root
        du = shutil.disk_usage(str(usage_target))

        service_ok, service_state = _service_is_active("stream-place-capture.service")

        payload: dict[str, Any] = {
            "ok": True,
            "timestamp_epoch_ms": now_ms,
            "service": {
                "name": "stream-place-capture.service",
                "active": service_ok,
                "state": service_state,
            },
            "config": {
                "quality_preset": cfg.quality_preset,
                "prune_processed_segments": cfg.prune_processed_segments,
                "keep_recent_raw_segments": cfg.keep_recent_raw_segments,
            },
            "paths": {
                "capture_root": str(capture_root),
                "final_root": str(final_root),
                "log_root": str(log_root),
                "state_json": str(state_path),
            },
            "recording": {
                "started_epoch_ms": recording_started_ms,
                "duration_ms": recording_duration_ms,
                "duration_human": _human_duration(recording_duration_ms),
            },
            "storage": {
                "capture_bytes": capture_bytes,
                "final_bytes": final_bytes,
                "log_bytes": log_bytes,
                "total_saved_bytes": total_saved_bytes,
                "final_live_outputs": final_live_outputs,
                "final_checkpoint_outputs": final_checkpoint_outputs,
                "final_chunk_outputs": final_chunk_outputs,
                "filesystem_total_bytes": du.total,
                "filesystem_used_bytes": du.used,
                "filesystem_free_bytes": du.free,
            },
            "streams": streams,
            "server": {
                "started_epoch_ms": self.started_epoch_ms,
                "uptime_ms": now_ms - self.started_epoch_ms,
                "uptime_human": _human_duration(now_ms - self.started_epoch_ms),
            },
        }

        self.last_payload = payload
        self.last_payload_epoch_ms = now_ms
        return payload


class _Handler(BaseHTTPRequestHandler):
    ctx: StatusContext

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/status", "/healthz"}:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":false,"error":"not found"}')
            return

        payload = self.ctx.build_payload()
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stream.place JSON status endpoint")
    p.add_argument("--config", type=Path, default=Path("config/streams.json"), help="Path to config file")
    p.add_argument("--host", default="0.0.0.0", help="Bind host")
    p.add_argument("--port", type=int, default=11456, help="Bind port")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ctx = StatusContext(config_path=args.config.resolve(), started_epoch_ms=int(time.time() * 1000))
    _Handler.ctx = ctx
    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
