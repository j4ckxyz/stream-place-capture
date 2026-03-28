from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path

from .config import CaptureConfig
from .service import CaptureService, setup_logging


@dataclass
class RunnerStatus:
    running: bool
    last_error: str | None


class ServiceRunner:
    def __init__(self, cfg: CaptureConfig) -> None:
        self.cfg = cfg
        self._thread: threading.Thread | None = None
        self._service: CaptureService | None = None
        self._lock = threading.Lock()
        self._started_logging = False
        self._last_error: str | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self) -> bool:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return False
            if not self._started_logging:
                setup_logging(self.cfg.log_root)
                self._started_logging = True
            self._last_error = None
            self._thread = threading.Thread(target=self._thread_main, name="capture-service-thread", daemon=True)
            self._thread.start()
            return True

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            self._last_error = str(exc)

    async def _run_async(self) -> None:
        loop = asyncio.get_running_loop()
        service = CaptureService(self.cfg)
        with self._lock:
            self._service = service
            self._loop = loop
        try:
            await service.run()
        finally:
            with self._lock:
                self._service = None
                self._loop = None

    def stop(self, timeout_seconds: float = 15.0) -> bool:
        with self._lock:
            svc = self._service
            th = self._thread

        if svc:
            svc.request_stop()
        if th:
            th.join(timeout=timeout_seconds)
            return not th.is_alive()
        return True

    def graceful_stop(self, timeout_seconds: float = 60.0) -> tuple[bool, dict[str, list[Path]]]:
        with self._lock:
            svc = self._service
            th = self._thread
            loop = self._loop

        if svc is None or loop is None:
            return True, {}

        async def _graceful() -> dict[str, list[Path]]:
            return await svc.graceful_stop()

        try:
            fut = asyncio.run_coroutine_threadsafe(_graceful(), loop)
            outputs = fut.result(timeout=timeout_seconds)
        except Exception:
            return False, {}

        if th:
            th.join(timeout=timeout_seconds)
            return (not th.is_alive()), outputs
        return True, outputs

    def checkpoint_and_stop(self, timeout_seconds: float = 45.0) -> bool:
        with self._lock:
            svc = self._service
            th = self._thread
            loop = self._loop

        if svc is None or loop is None:
            return True

        async def _checkpoint() -> None:
            await svc.checkpoint_and_stop()

        try:
            fut = asyncio.run_coroutine_threadsafe(_checkpoint(), loop)
            fut.result(timeout=timeout_seconds)
        except Exception:
            return False

        if th:
            th.join(timeout=timeout_seconds)
            return not th.is_alive()
        return True

    def status(self) -> RunnerStatus:
        with self._lock:
            running = bool(self._thread and self._thread.is_alive())
            return RunnerStatus(running=running, last_error=self._last_error)
