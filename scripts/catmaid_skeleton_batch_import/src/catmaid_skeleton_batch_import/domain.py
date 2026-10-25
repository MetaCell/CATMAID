"""Dependency-free importer domain objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .statuses import DatabaseStatus


@dataclass(frozen=True)
class IngestionRequest:
    schema_version: int
    archive_path: Path
    coordinate_unit: str
    target_project_id: int

    @property
    def scale_nm(self) -> float:
        return 1.0 if self.coordinate_unit == "nm" else 1000.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source": {
                "archive_path": str(self.archive_path),
                "coordinate_unit": self.coordinate_unit,
            },
            "target": {"project_id": self.target_project_id},
        }


@dataclass(frozen=True)
class ImportSettings:
    """Planning limits; execution reads batches from the retained plan."""
    max_nodes_per_batch: int
    max_archive_bytes: int
    max_swc_bytes: int
    max_total_nodes: int | None

    def plan_dict(self) -> dict[str, Any]:
        return {
            "max_nodes_per_batch": self.max_nodes_per_batch,
            "max_archive_bytes": self.max_archive_bytes,
            "max_swc_bytes": self.max_swc_bytes,
            "max_total_nodes": self.max_total_nodes,
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
    fragment_id: str
    batch_index: int | None = None

    @property
    def import_name(self) -> str:
        """Sanitize the COPY NULL marker in labels without changing source IDs."""
        return "_N" if self.display_name == r"\N" else self.display_name

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "member_path": self.member_path,
            "node_count": self.node_count,
            "uncompressed_bytes": self.uncompressed_bytes,
            "crc32": self.crc32,
            "display_name": self.display_name,
            "cable_length_nm": self.cable_length_nm,
            "fragment_id": self.fragment_id,
        }
        if self.batch_index is not None:
            result["batch_index"] = self.batch_index
        return result


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
            "status": DatabaseStatus.PLANNED,
        }
