"""Create the importer-owned hidden CATMAID project and metadata stack."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import Any


SYSTEM_PROJECT_PERMISSIONS = (
    "can_browse",
    "can_annotate",
    "can_annotate_with_token",
    "can_import",
    "can_administer",
)


class ProjectSetupError(RuntimeError):
    """The configured CATMAID deployment cannot initialize an import project."""


def _positive_triple(
    values: Sequence[int | float], label: str, *, integer: bool
) -> tuple[int, int, int] | tuple[float, float, float]:
    if len(values) != 3:
        raise ValueError(f"{label} must contain exactly three values")
    normalized: list[int | float] = []
    for value in values:
        if isinstance(value, bool):
            raise ValueError(f"{label} values must be positive")
        if integer:
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"{label} values must be positive integers")
            normalized.append(value)
        else:
            number = float(value)
            if not math.isfinite(number) or number <= 0:
                raise ValueError(f"{label} values must be positive finite numbers")
            normalized.append(number)
    return tuple(normalized)  # type: ignore[return-value]


def build_stack_metadata(cache_cell_size_nm: int = 1_500_000) -> dict[str, Any]:
    """Build the single spatial profile supported by importer v1."""

    if (
        isinstance(cache_cell_size_nm, bool)
        or not isinstance(cache_cell_size_nm, int)
        or cache_cell_size_nm <= 0
    ):
        raise ValueError("cache_cell_size_nm must be a positive integer")
    return {
        "cache_provider": "cached_msgpack_grid",
        "read_only": False,
        "spatial": [
            {
                "chunk_size": [cache_cell_size_nm] * 3,
                "limit": 0,
            }
        ],
    }


def _catmaid_components() -> dict[str, Any]:
    """Load CATMAID/Django components lazily for dependency-free imports."""

    from catmaid.apps import get_system_user
    from catmaid.control.common import get_class_to_id_map, get_relation_to_id_map
    from catmaid.control.project import validate_project_setup
    from catmaid.control.tracing import check_tracing_setup, setup_tracing
    from catmaid.models import Project, ProjectStack, Stack
    from django.db import transaction
    from guardian.shortcuts import assign_perm

    return {
        "get_system_user": get_system_user,
        "get_class_to_id_map": get_class_to_id_map,
        "get_relation_to_id_map": get_relation_to_id_map,
        "validate_project_setup": validate_project_setup,
        "check_tracing_setup": check_tracing_setup,
        "setup_tracing": setup_tracing,
        "Project": Project,
        "ProjectStack": ProjectStack,
        "Stack": Stack,
        "atomic": transaction.atomic,
        "assign_perm": assign_perm,
    }


def create_hidden_project(
    *,
    project_id: int,
    stack_id: int,
    project_stack_id: int,
    title: str,
    dimension: Sequence[int],
    resolution_nm: Sequence[int | float],
    cache_cell_size_nm: int = 1_500_000,
    comment: str | None = None,
    before_commit: Callable[[], None] | None = None,
    components: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new hidden project and stack in one durable transaction.

    IDs must have been consumed from their CATMAID sequences and durably saved
    before this call.  This function is create-only: primary-key or other
    conflicts are errors and it never looks up an object by title.
    """

    ids = (project_id, stack_id, project_stack_id)
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in ids):
        raise ValueError("project, stack, and project-stack IDs must be positive integers")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must be a non-empty string")

    normalized_dimension = _positive_triple(dimension, "dimension", integer=True)
    normalized_resolution = _positive_triple(
        resolution_nm, "resolution_nm", integer=False
    )
    metadata = build_stack_metadata(cache_cell_size_nm)
    canary = tuple(value // 2 for value in normalized_dimension)
    components = components or _catmaid_components()

    system_user = components["get_system_user"]()
    with components["atomic"](durable=True):
        project = components["Project"].objects.create(
            id=project_id,
            title=title,
            comment=comment
            or f"Hidden skeleton batch import project for {title}.",
        )

        # The Project post-save signal normally performs the first call.  Keep
        # this explicit idempotent setup because it is a hard importer
        # precondition and signals can be disabled in tests/fixtures.
        components["validate_project_setup"](
            project.id, system_user.id, fix=True
        )
        components["setup_tracing"](project.id, system_user)
        if not components["check_tracing_setup"](project.id):
            raise ProjectSetupError("CATMAID tracing setup is incomplete")

        stack = components["Stack"].objects.create(
            id=stack_id,
            title=title,
            dimension=normalized_dimension,
            resolution=normalized_resolution,
            comment=f"Metadata-only stack for {title} skeleton import.",
            description=f"Coordinate space for {title}.",
            downsample_factors=None,
            metadata=metadata,
            canary_location=canary,
            placeholder_color=(0, 0, 0, 1),
        )
        project_stack = components["ProjectStack"].objects.create(
            id=project_stack_id,
            project=project,
            stack=stack,
            translation=(0.0, 0.0, 0.0),
            orientation=0,
        )

        # A fresh object has no inherited Guardian object grants.  Add grants
        # only for the configured CATMAID system user; publication is a later,
        # separately authorized operation.
        for permission in SYSTEM_PROJECT_PERMISSIONS:
            components["assign_perm"](permission, system_user, project)

        class_map = components["get_class_to_id_map"](
            project.id, ("neuron", "skeleton")
        )
        relation_map = components["get_relation_to_id_map"](
            project.id, ("model_of",)
        )
        missing = {
            "classes": sorted({"neuron", "skeleton"} - set(class_map)),
            "relations": sorted({"model_of"} - set(relation_map)),
        }
        if missing["classes"] or missing["relations"]:
            raise ProjectSetupError(
                f"CATMAID tracing identifiers are missing: {missing}"
            )
        (before_commit or (lambda: None))()

    return {
        "project_id": int(project.id),
        "stack_id": int(stack.id),
        "project_stack_id": int(project_stack.id),
        "user_id": int(system_user.id),
        "system_user_id": int(system_user.id),
        "neuron_class_id": int(class_map["neuron"]),
        "skeleton_class_id": int(class_map["skeleton"]),
        "model_of_relation_id": int(relation_map["model_of"]),
        "metadata": metadata,
    }
