from __future__ import annotations

import io
import json
import os
import subprocess
import threading
import time
import tkinter as tk
import urllib.parse
import urllib.request
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any

from .config import CaptureConfig, StreamTarget
from .config import save_config
from .remux import Remuxer, estimated_gb_per_hour, list_quality_presets, quality_preset_info
from .runtime import ServiceRunner

try:
    from PIL import Image, ImageTk  # type: ignore[import-not-found]

    _HAS_PIL = True
except Exception:
    Image = None
    ImageTk = None
    _HAS_PIL = False


@dataclass
class UiCaptureState:
    status: str
    segments: int
    bytes_total: int
    last_segment_epoch_ms: int | None
    reconnects: int
    last_error: str | None


@dataclass
class UiLiveState:
    is_live: bool
    title: str | None
    thumb_cid: str | None
    thumb_bytes: bytes | None
    live_handle: str | None
    live_did: str | None


def _fmt_age_ms(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "never"
    age = max(0, int(time.time() * 1000) - epoch_ms)
    if age < 1000:
        return f"{age}ms"
    sec = age // 1000
    if sec < 120:
        return f"{sec}s"
    minute = sec // 60
    if minute < 120:
        return f"{minute}m"
    hour = minute // 60
    return f"{hour}h"


def _fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, n))
    i = 0
    while value >= 1024 and i < len(units) - 1:
        value /= 1024
        i += 1
    return f"{value:.1f}{units[i]}"


def _fmt_duration_ms(ms: int) -> str:
    ms = max(0, ms)
    sec_total, rem_ms = divmod(ms, 1000)
    days, rem = divmod(sec_total, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    return f"{days}d {hours}h {mins}m {secs}s {rem_ms}ms"


class LiveStatusPoller:
    def __init__(self, endpoint: str, targets: list[StreamTarget], interval_seconds: int = 15) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.targets = targets
        self.interval_seconds = max(5, interval_seconds)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state: dict[str, UiLiveState] = {
            t.name: UiLiveState(
                is_live=False,
                title=None,
                thumb_cid=None,
                thumb_bytes=None,
                live_handle=None,
                live_did=None,
            )
            for t in targets
        }
        self._blob_cache: dict[str, bytes] = {}

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="live-status-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def snapshot(self) -> dict[str, UiLiveState]:
        with self._lock:
            return dict(self._state)

    def _get_json(self, url: str) -> dict:
        with urllib.request.urlopen(url, timeout=12) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _get_blob(self, did: str, cid: str) -> bytes | None:
        key = f"{did}:{cid}"
        if key in self._blob_cache:
            return self._blob_cache[key]
        url = f"{self.endpoint}/xrpc/com.atproto.sync.getBlob?{urllib.parse.urlencode({'did': did, 'cid': cid})}"
        try:
            with urllib.request.urlopen(url, timeout=12) as resp:
                data = resp.read()
        except Exception:
            return None
        self._blob_cache[key] = data
        if len(self._blob_cache) > 48:
            first = next(iter(self._blob_cache.keys()))
            self._blob_cache.pop(first, None)
        return data

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                url = f"{self.endpoint}/xrpc/place.stream.live.getLiveUsers?limit=200"
                payload = self._get_json(url)

                by_did: dict[str, dict] = {}
                by_handle: dict[str, dict] = {}
                for item in payload.get("streams", []) or []:
                    author = item.get("author") or {}
                    record = item.get("record") or {}
                    did = str(author.get("did", "")).strip()
                    handle = str(author.get("handle", "")).strip().lstrip("@")
                    if did:
                        by_did[did] = item
                    if handle:
                        by_handle[handle] = item

                next_state: dict[str, UiLiveState] = {}
                for target in self.targets:
                    hit = by_did.get(target.did) or by_handle.get(target.handle)
                    prev = self._state.get(target.name)
                    if not hit:
                        next_state[target.name] = UiLiveState(
                            is_live=False,
                            title=None,
                            thumb_cid=prev.thumb_cid if prev else None,
                            thumb_bytes=prev.thumb_bytes if prev else None,
                            live_handle=None,
                            live_did=None,
                        )
                        continue

                    author = hit.get("author") or {}
                    record = hit.get("record") or {}
                    title = str(record.get("title") or "") or None
                    live_handle = str(author.get("handle") or "") or None
                    live_did = str(author.get("did") or "") or None

                    thumb = record.get("thumb") or {}
                    thumb_ref = thumb.get("ref") or {}
                    thumb_cid = thumb_ref.get("$link")
                    thumb_bytes = prev.thumb_bytes if prev else None
                    if thumb_cid and live_did:
                        if not prev or prev.thumb_cid != thumb_cid:
                            blob = self._get_blob(live_did, thumb_cid)
                            if blob:
                                thumb_bytes = blob

                    next_state[target.name] = UiLiveState(
                        is_live=True,
                        title=title,
                        thumb_cid=thumb_cid,
                        thumb_bytes=thumb_bytes,
                        live_handle=live_handle,
                        live_did=live_did,
                    )

                with self._lock:
                    self._state = next_state

            except Exception:
                pass

            self._stop.wait(self.interval_seconds)


class StreamCard:
    def __init__(self, parent: ttk.Frame, stream_name: str, handle: str, did: str) -> None:
        self.stream_name = stream_name
        self.handle = handle
        self.did = did
        self.preview_photo = None

        self.outer = ttk.Frame(parent, style="Card.TFrame", padding=12)
        self.outer.pack(fill="x", padx=10, pady=8)

        top = ttk.Frame(self.outer, style="Card.TFrame")
        top.pack(fill="x")
        ttk.Label(top, text=stream_name, style="CardTitle.TLabel").pack(side="left")
        ttk.Label(top, text=f"@{handle}", style="Muted.TLabel").pack(side="left", padx=(10, 0))

        self.live_badge = tk.Label(top, text="OFFLINE", bg="#6b7280", fg="#ffffff", padx=8, pady=2)
        self.live_badge.pack(side="right")

        self.preview = tk.Label(
            self.outer,
            text="No live preview",
            bg="#111827",
            fg="#e5e7eb",
            width=52,
            height=10,
            anchor="center",
        )
        self.preview.pack(fill="x", pady=(8, 10))

        self.streaming_var = tk.StringVar(value="stream.place live: no")
        self.capture_var = tk.StringVar(value="capture status: waiting")
        self.title_var = tk.StringVar(value="title: -")
        self.segment_var = tk.StringVar(value="segments: 0")
        self.bytes_var = tk.StringVar(value="captured: 0B")
        self.last_seg_var = tk.StringVar(value="last segment: never")
        self.reconnect_var = tk.StringVar(value="reconnects: 0")
        self.error_var = tk.StringVar(value="last error: none")

        for var in (
            self.streaming_var,
            self.capture_var,
            self.title_var,
            self.segment_var,
            self.bytes_var,
            self.last_seg_var,
            self.reconnect_var,
            self.error_var,
        ):
            ttk.Label(self.outer, textvariable=var, style="Body.TLabel").pack(fill="x", pady=1)
        ttk.Label(self.outer, text=f"did: {did}", style="Muted.TLabel").pack(fill="x", pady=(2, 0))

    def set_preview(self, photo: Any) -> None:
        if photo is None:
            self.preview_photo = None
            self.preview.configure(image="", text="No live preview")
            return
        self.preview_photo = photo
        self.preview.configure(image=photo, text="")

    def update(self, capture_state: UiCaptureState | None, live_state: UiLiveState | None) -> None:
        ls = live_state
        live_yes = bool(ls and ls.is_live)
        self.streaming_var.set(f"stream.place live: {'yes' if live_yes else 'no'}")

        if live_yes:
            self.live_badge.configure(text="LIVE", bg="#16a34a")
            if ls and ls.title:
                self.title_var.set(f"title: {ls.title[:120]}")
            else:
                self.title_var.set("title: -")
        else:
            self.live_badge.configure(text="OFFLINE", bg="#6b7280")
            self.title_var.set("title: -")

        if capture_state is None:
            self.capture_var.set("capture status: waiting")
            self.segment_var.set("segments: 0")
            self.bytes_var.set("captured: 0B")
            self.last_seg_var.set("last segment: never")
            self.reconnect_var.set("reconnects: 0")
            self.error_var.set("last error: none")
            return

        self.capture_var.set(f"capture status: {capture_state.status}")
        self.segment_var.set(f"segments: {capture_state.segments}")
        self.bytes_var.set(f"captured: {_fmt_bytes(capture_state.bytes_total)}")
        self.last_seg_var.set(f"last segment: {_fmt_age_ms(capture_state.last_segment_epoch_ms)} ago")
        self.reconnect_var.set(f"reconnects: {capture_state.reconnects}")
        self.error_var.set(f"last error: {capture_state.last_error or 'none'}")


class CaptureDashboard:
    def __init__(self, cfg: CaptureConfig, config_path: Path | None = None) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.runner = ServiceRunner(cfg)
        self.poller = LiveStatusPoller(cfg.endpoint, cfg.stream_targets, interval_seconds=15)
        self.root = tk.Tk()
        self.root.title("Stream.place Recorder")
        self.root.geometry("980x740")
        self.root.minsize(880, 620)
        self.root.configure(bg="#eef2f7")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self._setup_style()
        self.state_file = cfg.log_root / "state.json"
        self.cards: dict[str, StreamCard] = {}
        self.image_cache: dict[str, object] = {}
        self.capture_root_var = tk.StringVar()
        self.final_root_var = tk.StringVar()
        self.log_root_var = tk.StringVar()
        self.quality_var = tk.StringVar()
        self.quality_hint_var = tk.StringVar()
        self.runtime_var = tk.StringVar(value="Runtime: 0d 0h 0m 0s 0ms")
        self.total_size_var = tk.StringVar(value="Total saved: 0B")
        self.started_epoch_ms = int(time.time() * 1000)
        self._closing = False
        self._last_size_refresh_ms = 0
        self._cached_saved_size_bytes = 0

        self._build_ui()
        self._refresh_path_labels()
        self._refresh_quality_labels()
        self.poller.start()
        self.start_service(silent=True)
        self._tick()

    def _setup_style(self) -> None:
        style = ttk.Style(self.root)
        preferred = "vista" if "vista" in style.theme_names() else "clam"
        style.theme_use(preferred)
        style.configure("Card.TFrame", background="#ffffff")
        style.configure("CardTitle.TLabel", font=("Segoe UI", 14, "bold"), background="#ffffff", foreground="#0f172a")
        style.configure("Muted.TLabel", font=("Segoe UI", 9), background="#ffffff", foreground="#64748b")
        style.configure("Body.TLabel", font=("Segoe UI", 10), background="#ffffff", foreground="#0f172a")
        style.configure("Header.TLabel", font=("Segoe UI", 16, "bold"), background="#eef2f7", foreground="#0f172a")
        style.configure("Subhead.TLabel", font=("Segoe UI", 10), background="#eef2f7", foreground="#475569")

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root)
        top.pack(fill="x", padx=16, pady=(14, 8))

        left = ttk.Frame(top)
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, text="Stream.place Capture Control", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            left,
            text="Live previews, capture health, reconnect counters, and safe stop controls.",
            style="Subhead.TLabel",
        ).pack(anchor="w", pady=(2, 0))

        right = ttk.Frame(top)
        right.pack(side="right")
        ttk.Button(right, text="Start", command=self.start_service, width=12).pack(side="left", padx=4)
        ttk.Button(right, text="Stop", command=self.stop_service, width=12).pack(side="left", padx=4)
        ttk.Button(right, text="Rebuild Final", command=self.rebuild_final, width=14).pack(side="left", padx=4)
        ttk.Button(right, text="Change Save Folder", command=self.change_save_folder, width=18).pack(side="left", padx=4)

        quality_wrap = ttk.Frame(self.root)
        quality_wrap.pack(fill="x", padx=16, pady=(2, 4))
        ttk.Label(quality_wrap, text="Quality:", style="Subhead.TLabel").pack(side="left")
        preset_values = [p["name"] for p in list_quality_presets()]
        self.quality_combo = ttk.Combobox(quality_wrap, state="readonly", values=preset_values, textvariable=self.quality_var, width=14)
        self.quality_combo.pack(side="left", padx=(6, 6))
        self.quality_combo.bind("<<ComboboxSelected>>", self.change_quality)
        ttk.Button(quality_wrap, text="Build Preview", command=self.build_quality_preview, width=12).pack(side="left", padx=4)
        ttk.Label(quality_wrap, textvariable=self.quality_hint_var, style="Subhead.TLabel").pack(side="left", padx=8)

        self.service_status_var = tk.StringVar(value="service: stopped")
        ttk.Label(self.root, textvariable=self.service_status_var, style="Subhead.TLabel").pack(anchor="w", padx=18)

        totals_wrap = ttk.Frame(self.root)
        totals_wrap.pack(fill="x", padx=16, pady=(0, 2))
        ttk.Label(totals_wrap, textvariable=self.runtime_var, style="Subhead.TLabel").pack(side="left")
        ttk.Label(totals_wrap, text="  |  ", style="Subhead.TLabel").pack(side="left")
        ttk.Label(totals_wrap, textvariable=self.total_size_var, style="Subhead.TLabel").pack(side="left")

        paths_wrap = ttk.Frame(self.root)
        paths_wrap.pack(fill="x", padx=16, pady=(6, 2))
        ttk.Label(paths_wrap, textvariable=self.capture_root_var, style="Subhead.TLabel").pack(anchor="w")
        ttk.Label(paths_wrap, textvariable=self.final_root_var, style="Subhead.TLabel").pack(anchor="w")
        ttk.Label(paths_wrap, textvariable=self.log_root_var, style="Subhead.TLabel").pack(anchor="w")

        open_btns = ttk.Frame(self.root)
        open_btns.pack(fill="x", padx=16, pady=(0, 6))
        ttk.Button(open_btns, text="Open Raw Segments", command=lambda: self.open_folder(self.cfg.capture_root), width=18).pack(side="left", padx=(0, 6))
        ttk.Button(open_btns, text="Open Final Videos", command=lambda: self.open_folder(self.cfg.final_root), width=18).pack(side="left", padx=6)
        ttk.Button(open_btns, text="Open Logs", command=lambda: self.open_folder(self.cfg.log_root), width=14).pack(side="left", padx=6)

        scroller_wrap = ttk.Frame(self.root)
        scroller_wrap.pack(fill="both", expand=True, padx=8, pady=10)

        canvas = tk.Canvas(scroller_wrap, borderwidth=0, bg="#eef2f7", highlightthickness=0)
        scroll = ttk.Scrollbar(scroller_wrap, orient="vertical", command=canvas.yview)
        self.cards_frame = ttk.Frame(canvas)

        self.cards_frame.bind("<Configure>", lambda _: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.cards_frame, anchor="nw", width=940)
        canvas.configure(yscrollcommand=scroll.set)

        canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        for t in self.cfg.stream_targets:
            card = StreamCard(self.cards_frame, t.name, t.handle, t.did)
            self.cards[t.name] = card

    def _load_capture_state(self) -> dict[str, UiCaptureState]:
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

        out: dict[str, UiCaptureState] = {}
        for item in payload.get("streams", []):
            name = str(item.get("stream_name", "")).strip()
            if not name:
                continue
            out[name] = UiCaptureState(
                status=str(item.get("status", "unknown")),
                segments=int(item.get("segments", 0)),
                bytes_total=int(item.get("bytes_total", 0)),
                last_segment_epoch_ms=item.get("last_segment_epoch_ms"),
                reconnects=int(item.get("reconnects", 0)),
                last_error=(str(item.get("last_error")) if item.get("last_error") else None),
            )
        return out

    def _preview_photo(self, stream_name: str, live_state: UiLiveState | None):
        if not _HAS_PIL or Image is None or ImageTk is None or live_state is None or not live_state.thumb_bytes or not live_state.thumb_cid:
            return None

        key = f"{stream_name}:{live_state.thumb_cid}"
        cached = self.image_cache.get(key)
        if cached is not None:
            return cached

        try:
            img = Image.open(io.BytesIO(live_state.thumb_bytes)).convert("RGB")
            resample = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
            img = img.resize((848, 478), resample)
            photo = ImageTk.PhotoImage(img)
        except Exception:
            return None

        self.image_cache = {key: photo}
        return photo

    def _tick(self) -> None:
        run_status = self.runner.status()
        if run_status.running:
            self.service_status_var.set("service: running")
        else:
            self.service_status_var.set("service: stopped")
        if run_status.last_error:
            self.service_status_var.set(f"service error: {run_status.last_error}")

        capture_state = self._load_capture_state()
        live_state = self.poller.snapshot()

        for name, card in self.cards.items():
            cap = capture_state.get(name)
            live = live_state.get(name)
            card.update(cap, live)
            card.set_preview(self._preview_photo(name, live))

        self._refresh_runtime_totals(capture_state)

        self.root.after(1000, self._tick)

    def _refresh_path_labels(self) -> None:
        self.capture_root_var.set(f"Raw segments: {self.cfg.capture_root}")
        self.final_root_var.set(f"Final videos: {self.cfg.final_root}")
        self.log_root_var.set(f"Logs/state: {self.cfg.log_root}")

    def _refresh_quality_labels(self) -> None:
        info = quality_preset_info(self.cfg.quality_preset)
        self.quality_var.set(info["name"])
        est = estimated_gb_per_hour(info["name"])
        self.quality_hint_var.set(f"{info['label']} | est {est:.2f} GB/hr/stream")

    def _refresh_runtime_totals(self, capture_state: dict[str, UiCaptureState]) -> None:
        now_ms = int(time.time() * 1000)
        self.runtime_var.set(f"Runtime: {_fmt_duration_ms(now_ms - self.started_epoch_ms)}")
        if now_ms - self._last_size_refresh_ms >= 30000:
            self._cached_saved_size_bytes = self._compute_saved_size_bytes()
            self._last_size_refresh_ms = now_ms
        self.total_size_var.set(f"Total saved on disk: {_fmt_bytes(self._cached_saved_size_bytes)}")

    def _compute_saved_size_bytes(self) -> int:
        roots = {
            self.cfg.capture_root.resolve(),
            self.cfg.final_root.resolve(),
            self.cfg.log_root.resolve(),
        }
        total = 0
        for root in roots:
            if not root.exists():
                continue
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                try:
                    total += p.stat().st_size
                except Exception:
                    continue
        return total

    def change_quality(self, _event=None) -> None:
        selected = self.quality_var.get().strip().lower()
        if not selected:
            return
        if selected == self.cfg.quality_preset:
            self._refresh_quality_labels()
            return
        new_cfg = replace(self.cfg, quality_preset=selected)
        if self.runner.status().running:
            ok = messagebox.askyesno(
                "Apply quality preset",
                "Recorder will briefly restart to apply new quality preset. Continue?",
                icon=messagebox.WARNING,
            )
            if not ok:
                self._refresh_quality_labels()
                return
        self._apply_new_config(new_cfg, restart_if_running=True)
        self._refresh_quality_labels()

    def build_quality_preview(self) -> None:
        target = next(iter(self.cfg.stream_targets), None)
        if target is None:
            return
        remuxer = Remuxer(
            ffmpeg_path=self.cfg.ffmpeg_path,
            stream_name=target.name,
            segment_dir=self.cfg.capture_root / target.name / "segments",
            output_dir=self.cfg.final_root / target.name,
            quality_preset=self.cfg.quality_preset,
        )
        path = remuxer.build_preview_sample(self.cfg.quality_preset)
        if path is None:
            messagebox.showinfo("Preview", "Not enough recent segments to build preview yet.")
            return
        messagebox.showinfo("Preview built", f"Preview clip saved to:\n{path}")

    def open_folder(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                subprocess.run(["explorer", str(path)], check=False)
            else:
                raise RuntimeError("open folder supported on Windows only")
        except Exception:
            messagebox.showerror("Open folder", f"Could not open folder:\n{path}")

    def _apply_new_config(self, new_cfg: CaptureConfig, restart_if_running: bool = True) -> None:
        was_running = self.runner.status().running
        if was_running:
            stopped = self.runner.stop(timeout_seconds=25)
            if not stopped:
                messagebox.showwarning("Stop issue", "Service did not stop cleanly. Try again.")
                return

        self.cfg = new_cfg
        self.state_file = self.cfg.log_root / "state.json"
        self.runner = ServiceRunner(self.cfg)
        self._refresh_path_labels()
        self._refresh_quality_labels()
        self._last_size_refresh_ms = 0

        if self.config_path:
            try:
                save_config(self.config_path, self.cfg)
            except Exception as exc:
                messagebox.showwarning("Config save", f"Could not write config file:\n{exc}")

        if was_running and restart_if_running:
            self.start_service(silent=True)

    def change_save_folder(self) -> None:
        base_default = self.cfg.capture_root.parent if self.cfg.capture_root.parent != Path("") else Path(".")
        chosen = filedialog.askdirectory(title="Choose base storage folder", initialdir=str(base_default))
        if not chosen:
            return

        base = Path(chosen)
        new_cfg = replace(
            self.cfg,
            capture_root=base / "segments",
            final_root=base / "final",
            log_root=base / "logs",
        )

        if self.runner.status().running:
            ok = messagebox.askyesno(
                "Apply new save location",
                "Recorder will briefly restart to apply new save paths. Continue?",
                icon=messagebox.WARNING,
            )
            if not ok:
                return

        self._apply_new_config(new_cfg, restart_if_running=True)
        self._cached_saved_size_bytes = self._compute_saved_size_bytes()
        self.total_size_var.set(f"Total saved on disk: {_fmt_bytes(self._cached_saved_size_bytes)}")
        messagebox.showinfo(
            "Save location updated",
            f"New storage root:\n{base}\n\nRaw: {new_cfg.capture_root}\nFinal: {new_cfg.final_root}\nLogs: {new_cfg.log_root}",
        )

    def start_service(self, silent: bool = False) -> None:
        started = self.runner.start()
        if not started and not silent:
            messagebox.showinfo("Already running", "Capture service is already running.")

    def _close_window(self) -> None:
        self._closing = True
        self.poller.stop()
        try:
            self.root.destroy()
        except Exception:
            pass

    def stop_service(self) -> None:
        if self._closing:
            return
        if not self.runner.status().running:
            self._close_window()
            return

        ok = messagebox.askyesno(
            "Confirm stop",
            "This will finalize videos for each stream, stop capture, and close the app. Continue?",
            icon=messagebox.WARNING,
        )
        if not ok:
            return
        phrase = simpledialog.askstring("Double-check", 'Type STOP to confirm stopping capture:')
        if phrase != "STOP":
            messagebox.showwarning("Cancelled", "Stop cancelled (confirmation text did not match).")
            return

        stopped, outputs = self.runner.graceful_stop(timeout_seconds=70)
        if not stopped:
            messagebox.showwarning("Still stopping", "Service did not stop cleanly yet.")
            return

        lines = ["Capture stopped.", ""]
        has_outputs = False
        for stream_name, files in outputs.items():
            if files:
                has_outputs = True
                lines.append(f"{stream_name}:")
                for p in files:
                    lines.append(f"  - {p}")
        if not has_outputs:
            lines.append("No final output files were created yet.")
            lines.append(f"Raw segments are in: {self.cfg.capture_root}")
            lines.append("Use Rebuild Final before closing if you need an assembled clip now.")
            lines.append("")
            messagebox.showwarning("Stop complete with warning", "\n".join(lines))
            return
        lines.append("")
        lines.append("App will now close.")
        messagebox.showinfo("Stop complete", "\n".join(lines))
        self._close_window()

    def rebuild_final(self) -> None:
        from .remux import Remuxer

        def work() -> None:
            for t in self.cfg.stream_targets:
                remuxer = Remuxer(
                    ffmpeg_path=self.cfg.ffmpeg_path,
                    stream_name=t.name,
                    segment_dir=self.cfg.capture_root / t.name / "segments",
                    output_dir=self.cfg.final_root / t.name,
                    quality_preset=self.cfg.quality_preset,
                )
                remuxer.remux_progressive()

        threading.Thread(target=work, daemon=True).start()
        messagebox.showinfo("Remux", "Rebuild triggered in background.")

    def on_close(self) -> None:
        if self._closing:
            return
        self.stop_service()

    def run(self) -> None:
        self.root.mainloop()


def run_dashboard(cfg: CaptureConfig, config_path: Path | None = None) -> None:
    app = CaptureDashboard(cfg, config_path=config_path)
    app.run()
