from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pd2n8n import LoadError, Report, format_report, load, walk


def _replace_json(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    payload = json.dumps(document, indent=2, ensure_ascii=False) + "\n"
    tmp_path.write_text(payload, encoding="utf-8")
    os.replace(tmp_path, path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pd2n8n")
    parser.add_argument("input", type=Path, help="Pipedream workflow .json or project .zip")
    parser.add_argument("--out", type=Path, required=True, help="Output directory")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        loaded = load(args.input)
    except LoadError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    sections: list[tuple[str, Report]] = []
    for item in loaded.workflows:
        conversion = walk(item.workflow)
        out_path = args.out / f"{item.output_stem}.json"
        _replace_json(out_path, conversion.document)
        sections.append((item.workflow.name, conversion.report))

    print(
        format_report(
            sections,
            loaded.skipped_exports,
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
