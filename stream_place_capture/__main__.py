from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .config import load_config
from .gui import run_dashboard
from .remux import Remuxer
from .service import run_service


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream.place resilient segment recorder")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/streams.json"),
        help="Path to capture config JSON",
    )
    parser.add_argument(
        "--rebuild-final",
        action="store_true",
        help="Rebuild merged .live.mp4 outputs from saved raw segments and exit",
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Launch desktop dashboard (Windows-friendly)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    if args.rebuild_final:
        for target in cfg.stream_targets:
            remuxer = Remuxer(
                ffmpeg_path=cfg.ffmpeg_path,
                stream_name=target.name,
                segment_dir=cfg.capture_root / target.name / "segments",
                output_dir=cfg.final_root / target.name,
            )
            remuxer.remux_progressive()
        return
    if args.gui:
        run_dashboard(cfg, config_path=args.config)
        return
    asyncio.run(run_service(cfg))


if __name__ == "__main__":
    main()
