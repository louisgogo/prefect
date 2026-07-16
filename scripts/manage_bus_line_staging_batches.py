"""Administrator CLI for business-line staging batch activation and publication."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from modules.bus_line_staging import activate_batch, list_batches, publish_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List recent batches")
    list_parser.add_argument("--period", type=date.fromisoformat)
    list_parser.add_argument("--limit", type=int, default=50)

    activate_parser = subparsers.add_parser("activate", help="Set a READY batch as editable")
    activate_parser.add_argument("batch_id")

    publish_parser = subparsers.add_parser(
        "publish",
        help="Mark a FILLING batch published after fact_bus_line upload succeeds",
    )
    publish_parser.add_argument("batch_id")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "list":
        print(json.dumps(list_batches(args.period, args.limit), ensure_ascii=False, indent=2))
    elif args.command == "activate":
        activate_batch(args.batch_id)
        print(f"Activated batch {args.batch_id}")
    elif args.command == "publish":
        publish_batch(args.batch_id)
        print(f"Published batch {args.batch_id}")


if __name__ == "__main__":
    main()
