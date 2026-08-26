"""Bounded COPY row generation and atomic batch loading."""

from __future__ import annotations

import csv
import math
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Iterator, Sequence, TextIO

from .domain import ImportSettings, IngestionRequest, PlannedBatch, PlannedMember, SwcNode
from .errors import InvalidInputError, VerificationError


@dataclass(frozen=True)
class ProjectContext:
    project_id: int
    stack_id: int
    project_stack_id: int
    user_id: int
    neuron_class_id: int
    skeleton_class_id: int
    model_of_relation_id: int

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "ProjectContext":
        return cls(**{field: int(value[field]) for field in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


@dataclass(frozen=True)
class ExpectedSkeleton:
    member_path: str
    neuron_id: int
    skeleton_id: int
    link_id: int
    name: str
    node_count: int
    cable_length_nm: float

    def to_validation_dict(self) -> dict[str, object]:
        return {
            "member_path": self.member_path,
            "neuron_id": self.neuron_id,
            "skeleton_id": self.skeleton_id,
            "link_id": self.link_id,
            "name": self.name,
            "node_count": self.node_count,
            "cable_length_nm": self.cable_length_nm,
        }


class PreparedBatch:
    """COPY inputs and exact expectations for one bounded batch."""

    def __init__(
        self,
        *,
        batch: PlannedBatch,
        class_file: TextIO,
        relationship_file: TextIO,
        treenode_file: TextIO,
        expected_skeletons: tuple[ExpectedSkeleton, ...],
        location_ids: tuple[int, ...],
    ) -> None:
        self.batch = batch
        self.class_file = class_file
        self.relationship_file = relationship_file
        self.treenode_file = treenode_file
        self.expected_skeletons = expected_skeletons
        self.location_ids = location_ids

    @property
    def skeleton_ids(self) -> list[int]:
        return [expected.skeleton_id for expected in self.expected_skeletons]

    def rewind(self) -> None:
        self.class_file.seek(0)
        self.relationship_file.seek(0)
        self.treenode_file.seek(0)

    def close(self) -> None:
        self.class_file.close()
        self.relationship_file.close()
        self.treenode_file.close()

    def __enter__(self) -> "PreparedBatch":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _spooled_text_file() -> TextIO:
    return tempfile.SpooledTemporaryFile(  # type: ignore[return-value]
        max_size=8 * 1024 * 1024,
        mode="w+",
        encoding="utf-8",
        newline="",
    )


def _scaled_radius(radius: float, scale_nm: float) -> float:
    return -1.0 if radius == -1 else radius * scale_nm


def _expected_cable_nm(nodes: Sequence[SwcNode], scale_nm: float) -> float:
    by_id = {node.source_id: node for node in nodes}
    cable = 0.0
    for node in nodes:
        if node.parent_source_id == -1:
            continue
        parent = by_id[node.parent_source_id]
        cable += math.dist(
            (node.x * scale_nm, node.y * scale_nm, node.z * scale_nm),
            (parent.x * scale_nm, parent.y * scale_nm, parent.z * scale_nm),
        )
    return cable


def prepare_copy_rows(
    batch: PlannedBatch,
    request: IngestionRequest,
    project: ProjectContext,
    concept_ids: Sequence[int],
    location_ids: Sequence[int],
    load_nodes: Callable[[PlannedMember], Sequence[SwcNode]],
) -> PreparedBatch:
    expected_concepts = batch.skeleton_count * 3
    if len(concept_ids) != expected_concepts:
        raise InvalidInputError(
            f"batch {batch.index}: expected {expected_concepts} concept IDs, "
            f"found {len(concept_ids)}"
        )
    if len(location_ids) != batch.node_count:
        raise InvalidInputError(
            f"batch {batch.index}: expected {batch.node_count} location IDs, "
            f"found {len(location_ids)}"
        )
    if len(set(concept_ids)) != len(concept_ids):
        raise InvalidInputError(f"batch {batch.index}: concept IDs are not unique")
    if len(set(location_ids)) != len(location_ids):
        raise InvalidInputError(f"batch {batch.index}: location IDs are not unique")

    class_file = _spooled_text_file()
    relationship_file = _spooled_text_file()
    treenode_file = _spooled_text_file()
    class_writer = csv.writer(class_file, lineterminator="\n")
    relationship_writer = csv.writer(relationship_file, lineterminator="\n")
    treenode_writer = csv.writer(treenode_file, lineterminator="\n")

    concept_offset = 0
    location_offset = 0
    expected_skeletons: list[ExpectedSkeleton] = []
    try:
        for member in batch.members:
            nodes = tuple(load_nodes(member))
            if len(nodes) != member.node_count:
                raise InvalidInputError(
                    f"{member.member_path}: plan has {member.node_count} nodes, "
                    f"archive now yields {len(nodes)}"
                )
            computed_cable = _expected_cable_nm(nodes, request.scale_nm)
            tolerance = max(1e-6, abs(member.cable_length_nm) * 1e-12)
            if not math.isclose(
                computed_cable,
                member.cable_length_nm,
                rel_tol=1e-12,
                abs_tol=tolerance,
            ):
                raise InvalidInputError(
                    f"{member.member_path}: cable length no longer matches the plan"
                )

            neuron_id, skeleton_id, link_id = concept_ids[
                concept_offset : concept_offset + 3
            ]
            concept_offset += 3
            member_location_ids = location_ids[
                location_offset : location_offset + len(nodes)
            ]
            location_offset += len(nodes)
            node_id_map = {
                node.source_id: int(member_location_ids[index])
                for index, node in enumerate(nodes)
            }

            class_writer.writerow(
                [
                    neuron_id,
                    project.user_id,
                    project.project_id,
                    project.neuron_class_id,
                    member.display_name,
                ]
            )
            class_writer.writerow(
                [
                    skeleton_id,
                    project.user_id,
                    project.project_id,
                    project.skeleton_class_id,
                    member.display_name,
                ]
            )
            relationship_writer.writerow(
                [
                    link_id,
                    project.user_id,
                    project.project_id,
                    project.model_of_relation_id,
                    skeleton_id,
                    neuron_id,
                ]
            )

            for node in nodes:
                parent_id = (
                    r"\N"
                    if node.parent_source_id == -1
                    else node_id_map[node.parent_source_id]
                )
                treenode_writer.writerow(
                    [
                        node_id_map[node.source_id],
                        project.project_id,
                        repr(node.x * request.scale_nm),
                        repr(node.y * request.scale_nm),
                        repr(node.z * request.scale_nm),
                        project.user_id,
                        project.user_id,
                        skeleton_id,
                        repr(_scaled_radius(node.radius, request.scale_nm)),
                        parent_id,
                    ]
                )

            expected_skeletons.append(
                ExpectedSkeleton(
                    member_path=member.member_path,
                    neuron_id=int(neuron_id),
                    skeleton_id=int(skeleton_id),
                    link_id=int(link_id),
                    name=member.display_name,
                    node_count=len(nodes),
                    cable_length_nm=computed_cable,
                )
            )

        class_file.flush()
        relationship_file.flush()
        treenode_file.flush()
        prepared = PreparedBatch(
            batch=batch,
            class_file=class_file,
            relationship_file=relationship_file,
            treenode_file=treenode_file,
            expected_skeletons=tuple(expected_skeletons),
            location_ids=tuple(int(value) for value in location_ids),
        )
        prepared.rewind()
        return prepared
    except BaseException:
        class_file.close()
        relationship_file.close()
        treenode_file.close()
        raise


def _copy_file(cursor: object, sql: str, source: TextIO) -> None:
    source.seek(0)
    copy_expert = getattr(cursor, "copy_expert", None)
    if copy_expert is None and hasattr(cursor, "cursor"):
        copy_expert = getattr(cursor.cursor, "copy_expert")
    if copy_expert is None:
        raise RuntimeError("Configured PostgreSQL driver does not support COPY")
    copy_expert(sql, source)


def _iter_expected_treenodes(source: TextIO) -> Iterator[tuple[object, ...]]:
    source.seek(0)
    for row in csv.reader(source):
        yield (
            int(row[0]),
            int(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            int(row[5]),
            int(row[6]),
            int(row[7]),
            float(row[8]),
            None if row[9] == r"\N" else int(row[9]),
        )


def _treenode_row_matches(
    actual: Sequence[object], expected: Sequence[object]
) -> bool:
    """Compare exact identifiers and driver-rounded double-precision values."""

    if len(actual) != 10 or len(expected) != 10:
        return False
    exact_columns = (0, 1, 5, 6, 7, 9)
    if any(actual[index] != expected[index] for index in exact_columns):
        return False
    return all(
        math.isclose(
            float(actual[index]),
            float(expected[index]),
            rel_tol=1e-15,
            abs_tol=1e-9,
        )
        for index in (2, 3, 4, 8)
    )


def verify_exact_treenodes(cursor: object, prepared: PreparedBatch) -> None:
    cursor.execute(  # type: ignore[attr-defined]
        """
        SELECT t.id, t.project_id, t.location_x, t.location_y, t.location_z,
               t.editor_id, t.user_id, t.skeleton_id, t.radius, t.parent_id
        FROM unnest(%s::bigint[]) WITH ORDINALITY expected(id, position)
        LEFT JOIN treenode t ON t.id = expected.id
        ORDER BY expected.position
        """,
        [list(prepared.location_ids)],
    )
    expected_rows = _iter_expected_treenodes(prepared.treenode_file)
    count = 0
    while rows := cursor.fetchmany(10_000):  # type: ignore[attr-defined]
        for actual, expected in zip(rows, expected_rows):
            count += 1
            if actual[0] is None:
                raise VerificationError(
                    f"batch {prepared.batch.index}: missing treenode {expected[0]}"
                )
            if not _treenode_row_matches(actual, expected):
                raise VerificationError(
                    f"batch {prepared.batch.index}: treenode {expected[0]} "
                    "does not match its prepared COPY row",
                    details={"expected": expected, "actual": tuple(actual)},
                )
    try:
        extra_expected = next(expected_rows)
    except StopIteration:
        extra_expected = None
    if count != len(prepared.location_ids) or extra_expected is not None:
        raise VerificationError(
            f"batch {prepared.batch.index}: treenode verification count mismatch"
        )
    prepared.treenode_file.seek(0)


def execute_batch_transaction(
    prepared: PreparedBatch,
    project: ProjectContext,
    settings: ImportSettings,
    before_commit: Callable[[], None],
    record_timing: Callable[[str, float, dict[str, int]], None] | None = None,
) -> dict[str, int]:
    """COPY, materialize, validate, and attempt one atomic batch commit.

    ``before_commit`` must durably record the ambiguity marker. It is called as
    the last operation before leaving the outer atomic block.
    """
    from django.db import connection, transaction

    from .materialization import materialize_and_verify_batch

    timings = record_timing or (lambda _name, _duration, _counts: None)
    counts = {
        "skeletons": prepared.batch.skeleton_count,
        "nodes": prepared.batch.node_count,
        "class_instance_rows": prepared.batch.skeleton_count * 2,
        "relationship_rows": prepared.batch.skeleton_count,
        "treenode_rows": prepared.batch.node_count,
    }

    with transaction.atomic(durable=True):
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, true), "
                "set_config('lock_timeout', %s, true)",
                [str(settings.statement_timeout_ms), str(settings.lock_timeout_ms)],
            )
            cursor.execute("SET LOCAL session_replication_role = replica")
            copy_start = time.monotonic()
            _copy_file(
                cursor,
                "COPY class_instance "
                "(id, user_id, project_id, class_id, name) "
                "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                prepared.class_file,
            )
            _copy_file(
                cursor,
                "COPY class_instance_class_instance "
                "(id, user_id, project_id, relation_id, "
                "class_instance_a, class_instance_b) "
                "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                prepared.relationship_file,
            )
            _copy_file(
                cursor,
                "COPY treenode "
                "(id, project_id, location_x, location_y, location_z, "
                "editor_id, user_id, skeleton_id, radius, parent_id) "
                "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                prepared.treenode_file,
            )
            timings("copy", time.monotonic() - copy_start, counts)
            cursor.execute("SET LOCAL session_replication_role = origin")

            materialized = materialize_and_verify_batch(
                cursor=cursor,
                project_id=project.project_id,
                user_id=project.user_id,
                neuron_class_id=project.neuron_class_id,
                skeleton_class_id=project.skeleton_class_id,
                model_of_relation_id=project.model_of_relation_id,
                expected_skeletons=[
                    expected.to_validation_dict()
                    for expected in prepared.expected_skeletons
                ],
                location_ids=list(prepared.location_ids),
                record_timing=lambda name, duration: timings(
                    name, duration, counts
                ),
            )
            exact_validation_start = time.monotonic()
            verify_exact_treenodes(cursor, prepared)
            timings(
                "exact_treenode_validation",
                time.monotonic() - exact_validation_start,
                counts,
            )
            before_commit()

    result = dict(counts)
    result.update({key: int(value) for key, value in materialized.items()})
    return result
