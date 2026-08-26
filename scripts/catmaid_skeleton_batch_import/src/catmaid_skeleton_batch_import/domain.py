"""Dependency-free importer domain objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class IngestionRequest:
    schema_version: int
    archive_path: Path
    coordinate_unit: str
    dimension: tuple[int, int, int]
    resolution_nm: tuple[float, float, float]
    external_skeleton_id_map_path: Path | None = None

    @property
    def scale_nm(self) -> float:
        return 1.0 if self.coordinate_unit == "nm" else 1000.0

    @property
    def max_nm(self) -> tuple[float, float, float]:
        return tuple(
            float(dimension) * resolution
            for dimension, resolution in zip(self.dimension, self.resolution_nm)
        )  # type: ignore[return-value]

    def to_dict(self) -> dict[str, Any]:
        source: dict[str, Any] = {
            "archive_path": str(self.archive_path),
            "coordinate_unit": self.coordinate_unit,
        }
        if self.external_skeleton_id_map_path is not None:
            source["external_skeleton_id_map_path"] = str(
                self.external_skeleton_id_map_path
            )
        return {
            "schema_version": self.schema_version,
            "source": source,
            "stack": {
                "dimension": list(self.dimension),
                "resolution_nm": list(self.resolution_nm),
            },
        }


@dataclass(frozen=True)
class ImportSettings:
    max_nodes_per_batch: int
    max_archive_bytes: int
    max_swc_bytes: int
    max_swc_count: int
    max_total_nodes: int | None
    cache_cell_size_nm: int
    lod_levels: int
    lod_bucket_size: int
    lod_strategy: str
    statement_timeout_ms: int
    lock_timeout_ms: int

    def plan_dict(self) -> dict[str, Any]:
        """Values that must remain immutable after planning."""
        return {
            "max_nodes_per_batch": self.max_nodes_per_batch,
            "max_archive_bytes": self.max_archive_bytes,
            "max_swc_bytes": self.max_swc_bytes,
            "max_swc_count": self.max_swc_count,
            "max_total_nodes": self.max_total_nodes,
            "cache_cell_size_nm": self.cache_cell_size_nm,
            "lod_levels": self.lod_levels,
            "lod_bucket_size": self.lod_bucket_size,
            "lod_strategy": self.lod_strategy,
        }

    def operational_dict(self) -> dict[str, int]:
        return {
            "statement_timeout_ms": self.statement_timeout_ms,
            "lock_timeout_ms": self.lock_timeout_ms,
        }


@dataclass(frozen=True)
class SwcNode:
    source_id: int
    node_type: int
    x: float
    y: float
    z: float
    radius: float
    parent_source_id: int


@dataclass(frozen=True)
class SwcSummary:
    node_count: int
    cable_length_source_units: float


@dataclass(frozen=True)
class PlannedMember:
    member_path: str
    node_count: int
    uncompressed_bytes: int
    crc32: int
    display_name: str
    cable_length_nm: float
    external_skeleton_id: str | None = None
    batch_index: int | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "member_path": self.member_path,
            "node_count": self.node_count,
            "uncompressed_bytes": self.uncompressed_bytes,
            "crc32": self.crc32,
            "display_name": self.display_name,
            "cable_length_nm": self.cable_length_nm,
        }
        if self.external_skeleton_id is not None:
            result["external_skeleton_id"] = self.external_skeleton_id
        if self.batch_index is not None:
            result["batch_index"] = self.batch_index
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "PlannedMember":
        return cls(
            member_path=str(value["member_path"]),
            node_count=int(value["node_count"]),
            uncompressed_bytes=int(value["uncompressed_bytes"]),
            crc32=int(value["crc32"]),
            display_name=str(value["display_name"]),
            cable_length_nm=float(value["cable_length_nm"]),
            external_skeleton_id=(
                str(value["external_skeleton_id"])
                if value.get("external_skeleton_id") is not None
                else None
            ),
            batch_index=(
                int(value["batch_index"])
                if value.get("batch_index") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class PlannedBatch:
    index: int
    members: tuple[PlannedMember, ...]

    @property
    def node_count(self) -> int:
        return sum(member.node_count for member in self.members)

    @property
    def skeleton_count(self) -> int:
        return len(self.members)

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "first_member": self.members[0].member_path,
            "last_member": self.members[-1].member_path,
            "skeleton_count": self.skeleton_count,
            "node_count": self.node_count,
            "status": "planned",
        }
