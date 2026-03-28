from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .config import load_config
from .remux import Remuxer, estimated_gb_per_hour, list_quality_presets, quality_preset_info
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
    parser.add_argument(
        "--quality",
        choices=[p["name"] for p in list_quality_presets()],
        help="Temporarily override quality preset for this run",
    )
    parser.add_argument(
        "--list-quality",
        action="store_true",
        help="Print quality presets with estimated storage and exit",
    )
    parser.add_argument(
        "--build-preview",
        action="store_true",
        help="Build short preview clip(s) from recent segments and exit",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    if args.list_quality:
        for p in list_quality_presets():
            print(f"{p['name']}: {p['label']} | est {estimated_gb_per_hour(p['name']):.2f} GB/hr/stream | {p['description']}")
        return

    if args.quality:
        from dataclasses import replace

        cfg = replace(cfg, quality_preset=args.quality)
        info = quality_preset_info(cfg.quality_preset)
        print(f"Using quality preset: {info['name']} ({info['label']}) est {estimated_gb_per_hour(info['name']):.2f} GB/hr/stream")
    if args.rebuild_final:
        for target in cfg.stream_targets:
            remuxer = Remuxer(
                ffmpeg_path=cfg.ffmpeg_path,
                stream_name=target.name,
                segment_dir=cfg.capture_root / target.name / "segments",
                output_dir=cfg.final_root / target.name,
                quality_preset=cfg.quality_preset,
            )
            remuxer.remux_progressive()
        return
    if args.build_preview:
        for target in cfg.stream_targets:
            remuxer = Remuxer(
                ffmpeg_path=cfg.ffmpeg_path,
                stream_name=target.name,
                segment_dir=cfg.capture_root / target.name / "segments",
                output_dir=cfg.final_root / target.name,
                quality_preset=cfg.quality_preset,
            )
            path = remuxer.build_preview_sample(cfg.quality_preset)
            if path is None:
                print(f"{target.name}: no preview generated (not enough segments)")
            else:
                print(f"{target.name}: preview saved to {path}")
        return
    if args.gui:
        from .gui import run_dashboard

        run_dashboard(cfg, config_path=args.config)
        return
    asyncio.run(run_service(cfg))


if __name__ == "__main__":
    main()
