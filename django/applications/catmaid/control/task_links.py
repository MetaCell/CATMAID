# -*- coding: utf-8 -*-

"""Atomically assign an unlinked NeuroGlass task to a skeleton."""

from dataclasses import dataclass
import re

from django.db import connection, transaction
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse
from rest_framework.decorators import api_view

from catmaid.control import annotation, skeleton
from catmaid.control.authentication import PermissionError, requires_user_role
from catmaid.error import ClientError
from catmaid.models import ClassInstance, ClassInstanceClassInstance, Treenode, UserRole

UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TASK_ANNOTATION = re.compile(r'neuroglass:study:(%s):task:%s' % (UUID_PATTERN, UUID_PATTERN),
        re.IGNORECASE)


class TaskLinkConflict(ClientError):
    status_code = 409


@dataclass(frozen=True)
class TaskLink:
    annotation: str
    study_id: str
    skeleton_id: int
    reroot_treenode_id: int


def _parse_link(data) -> TaskLink:
    if not isinstance(data, dict) or data.keys() != {'annotation', 'skeleton_id', 'reroot_treenode_id'}:
        raise ValueError('Expected an annotation, a skeleton ID and a reroot treenode ID')
    name = data['annotation']
    match = TASK_ANNOTATION.fullmatch(name) if isinstance(name, str) else None
    if match is None:
        raise ValueError('Expected a NeuroGlass task annotation')
    for field in ('skeleton_id', 'reroot_treenode_id'):
        value = data[field]
        if type(value) is not int or not 0 < value < 2**63:
            raise ValueError('"%s" must be a positive integer in the supported range' % field)
    return TaskLink(name, match.group(1), data['skeleton_id'], data['reroot_treenode_id'])


def _target_neuron(project_id: int, skeleton_id: int) -> int:
    models = ClassInstanceClassInstance.objects.filter(
        project_id=project_id, relation__relation_name='model_of',
        class_instance_a__project_id=project_id, class_instance_a__class_column__class_name='skeleton',
        class_instance_b__project_id=project_id, class_instance_b__class_column__class_name='neuron',
    )
    targets = list(models.filter(class_instance_a_id=skeleton_id)
            .values_list('class_instance_b_id', flat=True)[:2])
    if len(targets) != 1:
        raise TaskLinkConflict('The target skeleton must model exactly one neuron')
    if models.filter(class_instance_b_id=targets[0]).exclude(class_instance_a_id=skeleton_id).exists():
        raise TaskLinkConflict('The target neuron must be modeled by exactly one skeleton')
    return targets[0]


def _assign(project_id: int, user, change: TaskLink) -> bool:
    # A task can be unlinked before its annotation row even exists. Serialize on its name.
    key = 'catmaid-task-link:%s:%s' % (project_id, change.annotation.lower())
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', [key])
    skeleton._lock_skeletons_for_reroot(project_id, [change.skeleton_id])

    # Match the task deletion guard's lock order. Check the final association under both locks.
    if not Treenode.objects.select_for_update().filter(
            project_id=project_id, id=change.reroot_treenode_id, skeleton_id=change.skeleton_id,
            ).exists():
        raise TaskLinkConflict('The reroot node is no longer on the target skeleton')

    neuron_id = _target_neuron(project_id, change.skeleton_id)
    if not ClassInstance.objects.select_for_update().filter(project_id=project_id, id=neuron_id).exists():
        raise TaskLinkConflict('The target neuron no longer exists')
    if _target_neuron(project_id, change.skeleton_id) != neuron_id:
        raise TaskLinkConflict('The target skeleton changed during assignment')

    links = ClassInstanceClassInstance.objects.filter(
        project_id=project_id, relation__relation_name='annotated_with',
        class_instance_a__class_column__class_name='neuron',
        class_instance_b__project_id=project_id, class_instance_b__class_column__class_name='annotation',
    )
    if links.filter(class_instance_b__name__iexact=change.annotation).exists():
        raise TaskLinkConflict('The task already has a skeleton')
    study_annotations = r'^neuroglass:study:%s:task:%s$' % (change.study_id, UUID_PATTERN)
    if links.filter(class_instance_a_id=neuron_id, class_instance_b__name__iregex=study_annotations).exists():
        raise TaskLinkConflict('The skeleton already belongs to another task in this study')

    added, _, _ = annotation._annotate_entities(project_id,
            [neuron_id], {change.annotation: {'user_id': user.id}})
    if not any(neuron_id in entities for entities in added.values()):
        raise TaskLinkConflict('The task annotation could not be added')
    return bool(skeleton._reroot_skeleton(change.reroot_treenode_id, project_id))


@api_view(['POST'])
@requires_user_role(UserRole.Annotate)
def link_task(request: HttpRequest, project_id=None) -> JsonResponse:
    """Assign an unlinked NeuroGlass task and reroot the skeleton atomically.

    Send one annotation name, destination skeleton ID, and reroot treenode ID.
    A successful response includes the assigned skeleton ID and whether its root changed.
    The task and destination must still be available when the transaction holds the locks.
    """
    if getattr(request, '_neuroglass_task_link_authorized', False) is not True:
        raise PermissionError('Task linking requires authorization from NeuroGlass')
    try:
        change = _parse_link(request.data)
    except ValueError as error:
        return HttpResponseBadRequest(str(error))

    with transaction.atomic():
        rerooted = _assign(int(project_id), request.user, change)
    return JsonResponse({'skeleton_id': change.skeleton_id, 'rerooted': rerooted})
