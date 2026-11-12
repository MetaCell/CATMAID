"""NeuroGlass task labels and protection during skeleton joins and deletion."""

import re

from django.db import connection

from catmaid.error import ClientError


PUBLIC_ID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
TASK_ANNOTATION_PREFIX = "neuroglass:study:"
TASK_ANNOTATION_PATTERN = (
    rf"^{TASK_ANNOTATION_PREFIX}{PUBLIC_ID_PATTERN}:task:{PUBLIC_ID_PATTERN}\Z"
)
TASK_ANNOTATION = re.compile(TASK_ANNOTATION_PATTERN, re.IGNORECASE)
TASK_ANNOTATION_NAMESPACE = re.compile(rf"^{TASK_ANNOTATION_PREFIX}[^:]*:task:", re.IGNORECASE)

MERGE_MESSAGE = (
    "This merge would remove a skeleton linked to a task. "
    "Choose the merge direction that keeps the linked skeleton, or change the task’s "
    "skeleton association before retrying."
)


class TaskSkeletonMergeConflict(ClientError):
    status_code = 409
    meta = {"code": "task_skeleton_merge_blocked"}


class TaskSkeletonDeletionConflict(ClientError):
    status_code = 409
    meta = {"code": "task_skeleton_final_node"}


def is_task_annotation(annotation_name: str | None) -> bool:
    return bool(annotation_name and TASK_ANNOTATION.fullmatch(annotation_name))


def task_annotations_by_neuron(project_id, neuron_ids) -> dict[int, dict[str, int]]:
    if not neuron_ids:
        return {}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT link.class_instance_a, annotation.name, link.user_id
            FROM class_instance_class_instance link
            JOIN relation r ON r.id = link.relation_id
            JOIN class_instance annotation ON annotation.id = link.class_instance_b
            JOIN class annotation_class ON annotation_class.id = annotation.class_id
            WHERE link.project_id = %s AND r.project_id = %s
                AND r.relation_name = 'annotated_with'
                AND link.class_instance_a = ANY(%s::bigint[])
                AND annotation.project_id = %s
                AND annotation_class.class_name = 'annotation'
                AND annotation.name ~* %s
            ORDER BY link.id
            """,
            [project_id, project_id, neuron_ids, project_id, TASK_ANNOTATION_PATTERN],
        )
        annotations: dict[int, dict[str, int]] = {}
        for neuron_id, name, user_id in cursor.fetchall():
            annotations.setdefault(neuron_id, {})[name] = user_id
    return annotations


def protect_task_annotations_on_join(project_id, winning_neuron_id, losing_neuron_id,
        annotation_map) -> dict[str, int] | None:
    """Protect task links after CATMAID has resolved and locked the join partners."""
    if not connection.in_atomic_block:
        raise RuntimeError("Task skeleton merge protection requires a transaction.")

    # CATMAID already locks both neurons FOR NO KEY UPDATE, which conflicts
    # with the FOR UPDATE locks taken by task annotation additions and removals.
    task_annotations = task_annotations_by_neuron(project_id, [winning_neuron_id, losing_neuron_id])
    if losing_neuron_id in task_annotations:
        raise TaskSkeletonMergeConflict(MERGE_MESSAGE)

    if annotation_map is None:
        return None  # CATMAID reads the current annotation union under the same locks.

    # CATMAID expands numbered templates later. Drop client-supplied names in
    # the task namespace and keep the survivor's stored links and annotators.
    selected = {
        name: user_id
        for name, user_id in annotation_map.items()
        if not TASK_ANNOTATION_NAMESPACE.match(name)
    }
    selected.update(task_annotations.get(winning_neuron_id, {}))
    return selected


def protect_task_annotations_on_delete(project_id, neuron_id) -> None:
    """Protect a skeleton being deleted while its neuron is locked."""
    if not connection.in_atomic_block:
        raise RuntimeError("Task skeleton deletion protection requires a transaction.")

    if task_annotations_by_neuron(project_id, [neuron_id]):
        raise TaskSkeletonDeletionConflict(
            "Skeleton cannot be deleted because it is associated with a task. "
            "Please link the task to a different skeleton before deleting it."
        )
