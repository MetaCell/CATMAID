"""Compare planning plus two source reads with identical runtime/resource limits.

Baseline uses the retained ZIP reader. Trial zero is warm-up. This measures
source processing only, not whole-import or cluster performance. No DB access.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from catmaid_skeleton_batch_import.archive import scan_archive
from catmaid_skeleton_batch_import.config import load_request, load_settings
from catmaid_skeleton_batch_import.orchestrator import _RunArchive, plan_ingestion
from catmaid_skeleton_batch_import.planner import plan_archive, read_plan
from catmaid_skeleton_batch_import.prepared_source import PreparedSourceReader


def measure(archive: Path, mode: str, coordinate_unit: str, trial: int) -> dict:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        request_path = root / "request.json"
        request_path.write_text(json.dumps({
            "schema_version": 1,
            "source": {"archive_path": str(archive.resolve()), "coordinate_unit": coordinate_unit},
            # Broad synthetic geometry isolates parsing from dataset-specific bounds.
            "stack": {"dimension": [1_000_000_000] * 3, "resolution_nm": [1000] * 3},
        }))
        request = load_request(request_path)
        settings = load_settings(use_django=False)
        start = time.monotonic()
        if mode == "baseline":
            plan = plan_archive(scan_archive(request, settings), settings)
        else:
            plan_ingestion(request_path, root / "state")
            plan = read_plan(root / "state/plan.jsonl.gz")
        planning = time.monotonic() - start
        start = time.monotonic()
        for _ in range(2):
            source = (_RunArchive(request, settings) if mode == "baseline"
                      else PreparedSourceReader(root / "state", plan))
            with source:
                actual = sum(len(source.load(member)) for member in plan.members)
                if actual != plan.total_nodes:
                    raise ValueError("Source node count differs from the plan")
        reads = time.monotonic() - start
        return {
            "mode": mode, "trial": trial, "warmup": trial == 0,
            "planning_seconds": planning, "two_read_passes_seconds": reads,
            "total_seconds": planning + reads,
            "cache_bytes": sum(p.stat().st_size for p in root.glob("state/prepared-*")),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--mode", choices=("baseline", "prepared"), required=True)
    parser.add_argument("--coordinate-unit", choices=("nm", "um"), default="um")
    parser.add_argument("--repeats", type=int, default=4, help="Includes one warm-up trial")
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("--repeats must include a warm-up and at least one measured trial")
    for trial in range(args.repeats):
        print(json.dumps(measure(args.archive, args.mode, args.coordinate_unit, trial)), flush=True)


if __name__ == "__main__":
    main()
