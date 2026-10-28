"""Resolve an existing CATMAID tracing project without provisioning data."""

from __future__ import annotations

from typing import Any


class ProjectSetupError(RuntimeError):
    """The requested target is not a configured CATMAID tracing project."""


def _catmaid_components() -> dict[str, Any]:
    """Load runtime dependencies after Django setup; tests can inject substitutes."""
    from catmaid.apps import get_system_user
    from catmaid.control.common import get_class_to_id_map, get_relation_to_id_map
    from catmaid.control.tracing import check_tracing_setup
    from catmaid.models import Project

    return {
        "get_system_user": get_system_user,
        "get_class_to_id_map": get_class_to_id_map,
        "get_relation_to_id_map": get_relation_to_id_map,
        "check_tracing_setup": check_tracing_setup,
        "Project": Project,
    }


def existing_project_context(
    project_id: int, *, components: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a configured tracing project without changing any of its data."""
    components = components or _catmaid_components()
    model = components["Project"]
    if not model.objects.filter(pk=project_id).exists():
        raise ProjectSetupError(f"Target project {project_id} does not exist")
    if not components["check_tracing_setup"](project_id):
        raise ProjectSetupError(f"Target project {project_id} has incomplete tracing setup")
    classes = components["get_class_to_id_map"](
        project_id, ("neuron", "skeleton", "annotation")
    )
    relations = components["get_relation_to_id_map"](
        project_id, ("model_of", "annotated_with")
    )
    if not {"neuron", "skeleton", "annotation"}.issubset(classes) or not {
        "model_of", "annotated_with"
    }.issubset(relations):
        raise ProjectSetupError(f"Target project {project_id} has incomplete tracing setup")
    return {
        "project_id": project_id,
        "user_id": int(components["get_system_user"]().id),
        "neuron_class_id": int(classes["neuron"]),
        "skeleton_class_id": int(classes["skeleton"]),
        "annotation_class_id": int(classes["annotation"]),
        "model_of_relation_id": int(relations["model_of"]),
        "annotated_with_relation_id": int(relations["annotated_with"]),
        "existing_project": True,
    }
