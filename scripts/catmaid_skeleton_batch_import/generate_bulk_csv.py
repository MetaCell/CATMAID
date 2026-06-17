#!/usr/bin/env python3
r"""Generate PostgreSQL COPY files for no-downtime CATMAID SWC imports.

The importer uses a two-pass batch contract:

1. Run with ``--scan-only`` to parse and validate the SWCs that belong to the
   next batch. This writes ``batch_plan.tsv`` and ``batch.env`` with exact
   skeleton/treenode counts.
2. Reserve the required CATMAID IDs by consuming values from ``concept_id_seq``
   and ``location_id_seq`` into ID list files.
3. Run again with the generated ``batch_plan.tsv`` plus reserved ID list files
   to write the CSV files and ``load.sql``.

``load.sql`` never repairs sequences with ``setval(max(id))``. That operation
is unsafe while CATMAID is online because concurrent writes can consume sequence
values between reservation and load. Sequence gaps after failed imports are
acceptable; sequence collisions are not.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, NamedTuple


class SwcNode(NamedTuple):
    source_id: int
    x: float
    y: float
    z: float
    radius: float
    parent_source_id: int


@dataclass(frozen=True)
class PlannedMember:
    zip_name: str
    member: str
    treenodes: int
    name: str


def env_default(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def required_int(value: str | None, name: str) -> int:
    if value is None:
        raise SystemExit(f"Missing required value: {name}")
    return int(value)


def required_tuple(value: tuple[float, float, float] | None, name: str) -> tuple[float, float, float]:
    if value is None:
        raise SystemExit(f"Missing required dataset config value: {name}")
    return value


def load_config(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    if not path.exists():
        raise SystemExit(f"Import config does not exist: {path}")
    with path.open() as config_file:
        return json.load(config_file)


def get_nested(config: dict[str, object], *keys: str) -> object | None:
    value: object = config
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def parse_3tuple(value: object | None, name: str) -> tuple[float, float, float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = list(value)
    else:
        raise SystemExit(f"{name} must be a three-item list or comma-separated string")
    if len(parts) != 3:
        raise SystemExit(f"{name} must have exactly three values")
    return (float(parts[0]), float(parts[1]), float(parts[2]))


def config_path(config: dict[str, object], *candidates: tuple[str, ...]) -> Path | None:
    for candidate in candidates:
        value = get_nested(config, *candidate)
        if value:
            return Path(str(value))
    return None


def resolve_swc_input(args: argparse.Namespace, config: dict[str, object]) -> Path | None:
    if args.swc_zip_dir is not None:
        return args.swc_zip_dir
    env_path = env_default("SWC_ZIP_DIR") or env_default("SWC_ZIP_PATH")
    if env_path:
        return Path(env_path)
    return config_path(
        config,
        ("swc_zip_path",),
        ("swc_zip_dir",),
        ("source", "swc_zip_path"),
        ("source", "swc_zip_dir"),
    )


def resolve_scale(args: argparse.Namespace, config: dict[str, object]) -> tuple[float, float, float] | None:
    if args.scale_x is not None and args.scale_y is not None and args.scale_z is not None:
        return (float(args.scale_x), float(args.scale_y), float(args.scale_z))

    env_scale = parse_3tuple(env_default("SWC_COORDINATE_SCALE_NM"), "SWC_COORDINATE_SCALE_NM")
    if env_scale is not None:
        return env_scale
    if all(env_default(name) is not None for name in ("SWC_SCALE_X", "SWC_SCALE_Y", "SWC_SCALE_Z")):
        return (
            float(env_default("SWC_SCALE_X", "0")),
            float(env_default("SWC_SCALE_Y", "0")),
            float(env_default("SWC_SCALE_Z", "0")),
        )

    return (
        parse_3tuple(get_nested(config, "coordinate_scale_nm"), "coordinate_scale_nm")
        or parse_3tuple(get_nested(config, "source", "coordinate_scale_nm"), "source.coordinate_scale_nm")
    )


def resolve_radius_scale(args: argparse.Namespace, config: dict[str, object]) -> float | None:
    if args.radius_scale is not None:
        return float(args.radius_scale)
    env_radius_scale = env_default("SWC_RADIUS_SCALE") or env_default("SWC_RADIUS_SCALE_NM")
    if env_radius_scale is not None:
        return float(env_radius_scale)
    value = get_nested(config, "radius_scale_nm")
    if value is None:
        value = get_nested(config, "source", "radius_scale_nm")
    return float(value) if value is not None else None


def resolve_source_bounds(args: argparse.Namespace, config: dict[str, object]) -> tuple[float, float, float] | None:
    if args.source_dim_x is not None and args.source_dim_y is not None and args.source_dim_z is not None:
        return (float(args.source_dim_x), float(args.source_dim_y), float(args.source_dim_z))

    env_bounds = parse_3tuple(env_default("SOURCE_COORDINATE_BOUNDS"), "SOURCE_COORDINATE_BOUNDS")
    if env_bounds is not None:
        return env_bounds
    if all(env_default(name) is not None for name in ("SOURCE_DIM_X", "SOURCE_DIM_Y", "SOURCE_DIM_Z")):
        return (
            float(env_default("SOURCE_DIM_X", "0")),
            float(env_default("SOURCE_DIM_Y", "0")),
            float(env_default("SOURCE_DIM_Z", "0")),
        )

    return (
        parse_3tuple(get_nested(config, "source_coordinate_bounds"), "source_coordinate_bounds")
        or parse_3tuple(get_nested(config, "source", "coordinate_bounds"), "source.coordinate_bounds")
    )


def sorted_zip_paths(path: Path) -> list[Path]:
    def key(zip_path: Path):
        return (0, int(zip_path.stem)) if zip_path.stem.isdigit() else (1, zip_path.name)

    if path.is_file():
        if path.suffix.lower() != ".zip":
            raise SystemExit(f"SWC input file is not a .zip archive: {path}")
        return [path]

    return sorted(path.glob("*.zip"), key=key)


def normalized_excluded_zips(excluded: list[str], excluded_csv: str | None) -> set[str]:
    names = set()
    for value in excluded:
        names.update(part.strip() for part in value.split(",") if part.strip())
    if excluded_csv:
        names.update(part.strip() for part in excluded_csv.split(",") if part.strip())

    normalized = set()
    for name in names:
        normalized.add(name)
        if not name.endswith(".zip"):
            normalized.add(f"{name}.zip")
    return normalized


def normalize_zip_name(name: str) -> str:
    return name if name.endswith(".zip") else f"{name}.zip"


def zip_paths_after_cursor(zip_paths: list[Path], after_zip: str | None) -> list[Path]:
    if not after_zip:
        return zip_paths

    normalized_after_zip = normalize_zip_name(after_zip)
    for index, zip_path in enumerate(zip_paths):
        if zip_path.name == normalized_after_zip:
            return zip_paths[index + 1:]
    raise SystemExit(f"--after-zip value was not found in input set: {normalized_after_zip}")


def parse_swc(text: str, label: str) -> list[SwcNode]:
    nodes: list[SwcNode] = []
    seen: set[int] = set()
    roots = 0
    root = None
    children: dict[int, list[int]] = {}

    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        row = line.split()
        if len(row) != 7:
            raise ValueError(f"{label}:{lineno}: expected 7 columns, got {len(row)}")

        source_id = int(row[0])
        if source_id in seen:
            raise ValueError(f"{label}:{lineno}: duplicate SWC node id {source_id}")
        seen.add(source_id)

        parent_source_id = int(row[6])
        if parent_source_id == -1:
            roots += 1
            root = source_id
        else:
            children.setdefault(parent_source_id, []).append(source_id)

        nodes.append(
            SwcNode(
                source_id=source_id,
                x=float(row[2]),
                y=float(row[3]),
                z=float(row[4]),
                radius=float(row[5]),
                parent_source_id=parent_source_id,
            )
        )

    if not nodes:
        raise ValueError(f"{label}: no SWC nodes found")
    if roots != 1:
        raise ValueError(f"{label}: expected exactly one root, found {roots}")

    source_ids = {node.source_id for node in nodes}
    missing = sorted(
        node.parent_source_id
        for node in nodes
        if node.parent_source_id != -1 and node.parent_source_id not in source_ids
    )
    if missing:
        raise ValueError(f"{label}: missing parent ids, first missing={missing[0]}")

    visited: set[int] = set()
    stack = [root]
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        stack.extend(children.get(node_id, ()))

    if len(visited) != len(nodes):
        raise ValueError(
            f"{label}: disconnected nodes or cycles found; "
            f"reached {len(visited)} of {len(nodes)} nodes from root"
        )

    return nodes


def validate_bounds(nodes: Iterable[SwcNode], label: str, dimension: tuple[float, float, float]) -> None:
    dim_x, dim_y, dim_z = dimension
    for node in nodes:
        if not (0 <= node.x <= dim_x and 0 <= node.y <= dim_y and 0 <= node.z <= dim_z):
            raise ValueError(
                f"{label}: node {node.source_id} outside source coordinate bounds: "
                f"x={node.x}, y={node.y}, z={node.z}"
            )


def sql_quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def write_load_sql(out_dir: Path, class_path: Path, link_path: Path, treenode_path: Path) -> None:
    (out_dir / "load.sql").write_text(
        f"""\\set ON_ERROR_STOP on
SET statement_timeout = 0;
SET lock_timeout = 0;

BEGIN;
\\copy class_instance (id, user_id, project_id, class_id, name) FROM '{sql_quote_path(class_path)}' WITH (FORMAT csv, NULL '\\N')
\\copy class_instance_class_instance (id, user_id, project_id, relation_id, class_instance_a, class_instance_b) FROM '{sql_quote_path(link_path)}' WITH (FORMAT csv, NULL '\\N')
\\copy treenode (id, project_id, location_x, location_y, location_z, editor_id, user_id, skeleton_id, radius, parent_id) FROM '{sql_quote_path(treenode_path)}' WITH (FORMAT csv, NULL '\\N')
COMMIT;

ANALYZE class_instance;
ANALYZE class_instance_class_instance;
ANALYZE treenode;
"""
    )


def output_bytes(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths if path.exists())


def write_batch_env(out_dir: Path, summary: dict[str, object]) -> None:
    values = {
        "BATCH_FIRST_ZIP": summary.get("first_zip", ""),
        "BATCH_LAST_ZIP": summary.get("last_zip", ""),
        "BATCH_NEXT_AFTER_ZIP": summary.get("next_after_zip", ""),
        "BATCH_COMPLETED_ALL_INPUT": str(summary["completed_all_input"]).lower(),
        "BATCH_SKELETONS": summary["skeletons"],
        "BATCH_TREENODES": summary["treenodes"],
        "BATCH_OUTPUT_BYTES": summary["output_bytes"],
    }
    if summary.get("batch_plan"):
        values["BATCH_PLAN"] = summary["batch_plan"]
    lines = [f"export {name}={shlex.quote(str(value))}" for name, value in values.items()]
    (out_dir / "batch.env").write_text("\n".join(lines) + "\n")


def write_batch_plan(path: Path, planned_members: list[PlannedMember]) -> None:
    with path.open("w", newline="") as plan_file:
        writer = csv.DictWriter(plan_file, fieldnames=["zip", "member", "treenodes", "name"], delimiter="\t")
        writer.writeheader()
        for planned in planned_members:
            writer.writerow(
                {
                    "zip": planned.zip_name,
                    "member": planned.member,
                    "treenodes": planned.treenodes,
                    "name": planned.name,
                }
            )


def read_batch_plan(path: Path) -> list[PlannedMember]:
    with path.open(newline="") as plan_file:
        reader = csv.DictReader(plan_file, delimiter="\t")
        return [
            PlannedMember(
                zip_name=row["zip"],
                member=row["member"],
                treenodes=int(row["treenodes"]),
                name=row["name"],
            )
            for row in reader
        ]


def read_reserved_ids(path: Path, expected_count: int, label: str) -> list[int]:
    ids: list[int] = []
    with path.open() as id_file:
        for lineno, raw in enumerate(id_file, 1):
            value = raw.strip()
            if not value:
                continue
            try:
                ids.append(int(value))
            except ValueError as exc:
                raise SystemExit(f"{label} reserved ID file has a non-integer on line {lineno}: {value}") from exc

    if len(ids) != expected_count:
        raise SystemExit(
            f"{label} reserved ID count mismatch: expected {expected_count}, found {len(ids)} in {path}"
        )
    if len(set(ids)) != len(ids):
        raise SystemExit(f"{label} reserved ID file contains duplicate IDs: {path}")
    return ids


def id_summary(ids: list[int]) -> dict[str, int]:
    return {
        "count": len(ids),
        "min": min(ids),
        "max": max(ids),
    }


def scan_batch(
    zip_paths: list[Path],
    dimension: tuple[float, float, float],
) -> tuple[list[PlannedMember], list[dict[str, str]]]:
    planned_members: list[PlannedMember] = []
    skipped: list[dict[str, str]] = []
    seen_names: dict[str, int] = {}

    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path) as zip_file:
            members = sorted(name for name in zip_file.namelist() if name.lower().endswith(".swc"))
            for member in members:
                label = f"{zip_path.name}:{member}"
                text = zip_file.read(member).decode("utf-8", errors="replace")
                try:
                    nodes = parse_swc(text, label)
                    validate_bounds(nodes, label, dimension)
                except Exception as exc:
                    skipped.append({"zip": zip_path.name, "member": member, "error": str(exc)})
                    continue

                base_name = Path(member).stem
                duplicate_index = seen_names.get(base_name, 0)
                seen_names[base_name] = duplicate_index + 1
                name = base_name if duplicate_index == 0 else f"{base_name}__{zip_path.stem}__{duplicate_index}"
                planned_members.append(
                    PlannedMember(
                        zip_name=zip_path.name,
                        member=member,
                        treenodes=len(nodes),
                        name=name[:255],
                    )
                )

    return planned_members, skipped


def selected_zip_paths(
    swc_input: Path,
    after_zip: str | None,
    excluded_zips: set[str],
    max_zip_files: int | None,
) -> tuple[list[Path], int, int, bool]:
    remaining = [
        zip_path
        for zip_path in zip_paths_after_cursor(sorted_zip_paths(swc_input), after_zip)
        if zip_path.name not in excluded_zips
    ]
    if not remaining:
        raise SystemExit(f"No .zip files found in {swc_input}")

    if max_zip_files is not None:
        if max_zip_files <= 0:
            raise SystemExit("--max-zip-files must be greater than zero")
        selected = remaining[:max_zip_files]
    else:
        selected = remaining

    completed_all_input = len(selected) == len(remaining)
    return selected, len(remaining), max(len(remaining) - len(selected), 0), completed_all_input


def write_scan_outputs(
    out_dir: Path,
    batch_plan_path: Path,
    planned_members: list[PlannedMember],
    skipped: list[dict[str, str]],
    zip_paths: list[Path],
    input_zip_files: int,
    remaining_zip_files: int,
    completed_all_input: bool,
    scale: tuple[float, float, float],
    radius_scale: float,
    dimension: tuple[float, float, float],
    excluded_zips: set[str],
    dataset_id: str | None,
) -> dict[str, object]:
    write_batch_plan(batch_plan_path, planned_members)
    first_zip = zip_paths[0].name if zip_paths else None
    last_zip = zip_paths[-1].name if zip_paths else None
    summary = {
        "dataset_id": dataset_id,
        "scan_only": True,
        "batch_plan": str(batch_plan_path),
        "first_zip": first_zip,
        "last_zip": last_zip,
        "next_after_zip": last_zip,
        "completed_all_input": completed_all_input,
        "processed_zip_files": len(zip_paths),
        "remaining_zip_files": remaining_zip_files,
        "output_bytes": output_bytes((batch_plan_path,)),
        "skeletons": len(planned_members),
        "treenodes": sum(planned.treenodes for planned in planned_members),
        "required_concept_ids": len(planned_members) * 3,
        "required_location_ids": sum(planned.treenodes for planned in planned_members),
        "skipped": len(skipped),
        "scale": list(scale),
        "radius_scale": radius_scale,
        "source_coordinate_bounds": list(dimension),
        "input_zip_files": input_zip_files,
        "excluded_zip_files": sorted(excluded_zips),
    }
    (out_dir / "scan_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    if skipped:
        (out_dir / "skipped.json").write_text(json.dumps(skipped, indent=2, sort_keys=True))
    write_batch_env(out_dir, summary)
    return summary


def generate_from_plan(
    planned_members: list[PlannedMember],
    zip_by_name: dict[str, Path],
    out_dir: Path,
    project_id: int,
    user_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    model_of_relation_id: int,
    concept_ids: list[int],
    location_ids: list[int],
    scale: tuple[float, float, float],
    radius_scale: float,
    dimension: tuple[float, float, float],
) -> dict[str, object]:
    class_path = out_dir / "class_instance.csv"
    link_path = out_dir / "class_instance_class_instance.csv"
    treenode_path = out_dir / "treenode.csv"
    manifest_path = out_dir / "manifest.tsv"

    for path in (
        class_path,
        link_path,
        treenode_path,
        manifest_path,
        out_dir / "load.sql",
    ):
        path.unlink(missing_ok=True)

    skeleton_count = 0
    treenode_count = 0
    concept_index = 0
    location_index = 0

    with class_path.open("w", newline="") as class_file, \
            link_path.open("w", newline="") as link_file, \
            treenode_path.open("w", newline="") as treenode_file, \
            manifest_path.open("w") as manifest_file:
        class_writer = csv.writer(class_file)
        link_writer = csv.writer(link_file)
        treenode_writer = csv.writer(treenode_file)
        manifest_file.write("zip\tmember\tneuron_id\tskeleton_id\ttreenodes\tname\n")

        current_zip_name = None
        zip_file: zipfile.ZipFile | None = None
        try:
            for planned in planned_members:
                if planned.zip_name != current_zip_name:
                    if zip_file is not None:
                        zip_file.close()
                    if planned.zip_name not in zip_by_name:
                        raise SystemExit(f"Planned zip is not present in input set: {planned.zip_name}")
                    zip_file = zipfile.ZipFile(zip_by_name[planned.zip_name])
                    current_zip_name = planned.zip_name

                assert zip_file is not None
                label = f"{planned.zip_name}:{planned.member}"
                text = zip_file.read(planned.member).decode("utf-8", errors="replace")
                nodes = parse_swc(text, label)
                validate_bounds(nodes, label, dimension)
                if len(nodes) != planned.treenodes:
                    raise ValueError(
                        f"{label}: planned {planned.treenodes} nodes, generated {len(nodes)} nodes"
                    )

                neuron_id = concept_ids[concept_index]
                skeleton_id = concept_ids[concept_index + 1]
                link_id = concept_ids[concept_index + 2]
                concept_index += 3

                class_writer.writerow([neuron_id, user_id, project_id, neuron_class_id, planned.name])
                class_writer.writerow([skeleton_id, user_id, project_id, skeleton_class_id, planned.name])
                link_writer.writerow([link_id, user_id, project_id, model_of_relation_id, skeleton_id, neuron_id])

                node_id_map = {}
                for node in nodes:
                    node_id_map[node.source_id] = location_ids[location_index]
                    location_index += 1

                for node in nodes:
                    parent_id = r"\N" if node.parent_source_id == -1 else node_id_map[node.parent_source_id]
                    treenode_writer.writerow([
                        node_id_map[node.source_id],
                        project_id,
                        f"{node.x * scale[0]:.6f}",
                        f"{node.y * scale[1]:.6f}",
                        f"{node.z * scale[2]:.6f}",
                        user_id,
                        user_id,
                        skeleton_id,
                        f"{node.radius * radius_scale:.6f}",
                        parent_id,
                    ])

                skeleton_count += 1
                treenode_count += len(nodes)
                manifest_file.write(
                    f"{planned.zip_name}\t{planned.member}\t{neuron_id}\t{skeleton_id}\t{len(nodes)}\t{planned.name}\n"
                )

                if skeleton_count % 10000 == 0:
                    print(f"generated {skeleton_count} skeletons, {treenode_count} treenodes", file=sys.stderr)
        finally:
            if zip_file is not None:
                zip_file.close()

    write_load_sql(out_dir, class_path, link_path, treenode_path)
    return {
        "output_bytes": output_bytes((class_path, link_path, treenode_path, manifest_path)),
        "skeletons": skeleton_count,
        "treenodes": treenode_count,
        "used_concept_ids": concept_index,
        "used_location_ids": location_index,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=env_default("IMPORT_CONFIG"), type=Path, required=False)
    parser.add_argument(
        "--swc-zip-dir",
        type=Path,
        required=False,
        help="Directory containing .zip files, or one .zip file. Can also be set with SWC_ZIP_DIR or SWC_ZIP_PATH.",
    )
    parser.add_argument("--staging", default=env_default("STAGING"), type=Path, required=False)
    parser.add_argument("--project-id", default=env_default("PROJECT_ID"), required=False)
    parser.add_argument("--import-user-id", default=env_default("IMPORT_USER_ID"), required=False)
    parser.add_argument("--neuron-class-id", default=env_default("NEURON_CLASS_ID"), required=False)
    parser.add_argument("--skeleton-class-id", default=env_default("SKELETON_CLASS_ID"), required=False)
    parser.add_argument("--model-of-relation-id", default=env_default("MODEL_OF_RELATION_ID"), required=False)
    parser.add_argument("--concept-ids-file", default=env_default("RESERVED_CONCEPT_IDS_FILE"), type=Path, required=False)
    parser.add_argument("--location-ids-file", default=env_default("RESERVED_LOCATION_IDS_FILE"), type=Path, required=False)
    parser.add_argument("--exclude-zip", action="append", default=[])
    parser.add_argument("--exclude-zips", default=env_default("EXCLUDE_ZIPS", ""), required=False)
    parser.add_argument("--after-zip", default=env_default("AFTER_ZIP"), required=False)
    parser.add_argument("--max-zip-files", default=env_default("BATCH_MAX_ZIPS"), required=False)
    parser.add_argument("--batch-plan", default=env_default("BATCH_PLAN"), type=Path, required=False)
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument("--scale-x", type=float, default=env_default("SWC_SCALE_X"))
    parser.add_argument("--scale-y", type=float, default=env_default("SWC_SCALE_Y"))
    parser.add_argument("--scale-z", type=float, default=env_default("SWC_SCALE_Z"))
    parser.add_argument("--radius-scale", type=float, default=env_default("SWC_RADIUS_SCALE"))
    parser.add_argument("--source-dim-x", "--dim-x", dest="source_dim_x", type=float, default=env_default("SOURCE_DIM_X"))
    parser.add_argument("--source-dim-y", "--dim-y", dest="source_dim_y", type=float, default=env_default("SOURCE_DIM_Y"))
    parser.add_argument("--source-dim-z", "--dim-z", dest="source_dim_z", type=float, default=env_default("SOURCE_DIM_Z"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    dataset_id = get_nested(config, "dataset_id")
    swc_input = resolve_swc_input(args, config)
    if swc_input is None:
        raise SystemExit("Missing SWC input path: set IMPORT_CONFIG, SWC_ZIP_DIR, or SWC_ZIP_PATH")
    if args.staging is None:
        raise SystemExit("Missing --staging or STAGING")
    if not swc_input.exists():
        raise SystemExit(f"SWC zip input path does not exist: {swc_input}")

    scale = required_tuple(resolve_scale(args, config), "coordinate_scale_nm")
    radius_scale = resolve_radius_scale(args, config)
    if radius_scale is None:
        raise SystemExit("Missing required dataset config value: radius_scale_nm")
    dimension = required_tuple(resolve_source_bounds(args, config), "source_coordinate_bounds")
    excluded_zips = normalized_excluded_zips(args.exclude_zip, args.exclude_zips)
    max_zip_files = int(args.max_zip_files) if args.max_zip_files else None

    out_dir = args.staging
    out_dir.mkdir(parents=True, exist_ok=True)

    cleanup_paths = [
        out_dir / "summary.json",
        out_dir / "skipped.json",
        out_dir / "batch.env",
    ]
    if args.scan_only:
        cleanup_paths.append(out_dir / "scan_summary.json")

    for path in cleanup_paths:
        path.unlink(missing_ok=True)

    if args.scan_only:
        zip_paths, input_zip_files, remaining_zip_files, completed_all_input = selected_zip_paths(
            swc_input,
            args.after_zip,
            excluded_zips,
            max_zip_files,
        )
        batch_plan_path = args.batch_plan or (out_dir / "batch_plan.tsv")
        planned_members, skipped = scan_batch(zip_paths, dimension)
        summary = write_scan_outputs(
            out_dir,
            batch_plan_path,
            planned_members,
            skipped,
            zip_paths,
            input_zip_files,
            remaining_zip_files,
            completed_all_input,
            scale,
            radius_scale,
            dimension,
            excluded_zips,
            str(dataset_id) if dataset_id is not None else None,
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
        return

    project_id = required_int(args.project_id, "PROJECT_ID")
    user_id = required_int(args.import_user_id, "IMPORT_USER_ID")
    neuron_class_id = required_int(args.neuron_class_id, "NEURON_CLASS_ID")
    skeleton_class_id = required_int(args.skeleton_class_id, "SKELETON_CLASS_ID")
    model_of_relation_id = required_int(args.model_of_relation_id, "MODEL_OF_RELATION_ID")

    if args.batch_plan is None:
        raise SystemExit("Missing --batch-plan or BATCH_PLAN. Run --scan-only before generation.")
    planned_members = read_batch_plan(args.batch_plan)
    if not planned_members:
        raise SystemExit(f"Batch plan contains no SWC members: {args.batch_plan}")
    if args.concept_ids_file is None:
        raise SystemExit("Missing --concept-ids-file or RESERVED_CONCEPT_IDS_FILE")
    if args.location_ids_file is None:
        raise SystemExit("Missing --location-ids-file or RESERVED_LOCATION_IDS_FILE")

    expected_concept_ids = len(planned_members) * 3
    expected_location_ids = sum(planned.treenodes for planned in planned_members)
    concept_ids = read_reserved_ids(args.concept_ids_file, expected_concept_ids, "concept")
    location_ids = read_reserved_ids(args.location_ids_file, expected_location_ids, "location")
    concept_id_summary = id_summary(concept_ids)
    location_id_summary = id_summary(location_ids)

    all_zip_paths = [
        zip_path
        for zip_path in sorted_zip_paths(swc_input)
        if zip_path.name not in excluded_zips
    ]
    zip_by_name = {zip_path.name: zip_path for zip_path in all_zip_paths}
    generation = generate_from_plan(
        planned_members,
        zip_by_name,
        out_dir,
        project_id,
        user_id,
        neuron_class_id,
        skeleton_class_id,
        model_of_relation_id,
        concept_ids,
        location_ids,
        scale,
        radius_scale,
        dimension,
    )

    first_zip = planned_members[0].zip_name
    last_zip = planned_members[-1].zip_name
    completed_all_input = str(env_default("BATCH_COMPLETED_ALL_INPUT", "true")).lower() == "true"
    summary = {
        "dataset_id": str(dataset_id) if dataset_id is not None else None,
        "scan_only": False,
        "batch_plan": str(args.batch_plan),
        "first_zip": first_zip,
        "last_zip": last_zip,
        "next_after_zip": last_zip,
        "completed_all_input": completed_all_input,
        "processed_zip_files": len({planned.zip_name for planned in planned_members}),
        "remaining_zip_files": None,
        "output_bytes": generation["output_bytes"],
        "skeletons": generation["skeletons"],
        "treenodes": generation["treenodes"],
        "required_concept_ids": generation["skeletons"] * 3,
        "required_location_ids": generation["treenodes"],
        "reserved_concept_ids_file": str(args.concept_ids_file),
        "reserved_location_ids_file": str(args.location_ids_file),
        "reserved_concept_id_count": concept_id_summary["count"],
        "reserved_concept_id_min": concept_id_summary["min"],
        "reserved_concept_id_max": concept_id_summary["max"],
        "reserved_location_id_count": location_id_summary["count"],
        "reserved_location_id_min": location_id_summary["min"],
        "reserved_location_id_max": location_id_summary["max"],
        "used_concept_ids": generation["used_concept_ids"],
        "used_location_ids": generation["used_location_ids"],
        "scale": list(scale),
        "radius_scale": radius_scale,
        "source_coordinate_bounds": list(dimension),
        "excluded_zip_files": sorted(excluded_zips),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    write_batch_env(out_dir, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
