# -*- coding: utf-8 -*-

"""Find and atomically assign the closest eligible skeleton to a NeuroGlass task."""

from dataclasses import dataclass
import math
import re
import sys

from django.db import connection, transaction
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser

from catmaid.control import annotation, skeleton
from catmaid.control.authentication import PermissionError, requires_user_role, user_domain
from catmaid.control.common import get_class_to_id_map, get_relation_to_id_map
from catmaid.error import ClientError
from catmaid.models import ClassInstance, ClassInstanceClassInstance, Treenode, UserRole
from catmaid.task_annotations import TASK_ANNOTATION, UUID_PATTERN

# Leave room for the search bounds and squared 3D distances in double precision.
MAX_SPATIAL_MAGNITUDE = math.sqrt(sys.float_info.max) / 4


class TaskLinkConflict(ClientError):
    status_code = 409


class TaskLinkNoMatch(TaskLinkConflict):
    meta = {'code': 'task_link_no_match'}


@dataclass(frozen=True)
class TaskLink:
    annotation: str
    study_id: str
    coordinate: tuple[float, float, float]
    radius: float


def _spatial_number(value) -> float:
    if type(value) not in (int, float) or not -MAX_SPATIAL_MAGNITUDE <= value <= MAX_SPATIAL_MAGNITUDE:
        raise ValueError('Expected a finite number in the supported spatial range')
    return float(value)


def _parse_link(data) -> TaskLink:
    if not isinstance(data, dict) or data.keys() != {'annotation', 'coordinate', 'radius'}:
        raise ValueError('Expected an annotation, one coordinate and a radius')
    name = data['annotation']
    match = re.fullmatch(TASK_ANNOTATION.pattern, name) if isinstance(name, str) else None
    if match is None:
        raise ValueError('Expected a NeuroGlass task annotation')
    coordinate = data['coordinate']
    if not isinstance(coordinate, list) or len(coordinate) != 3:
        raise ValueError('Expected one [x, y, z] coordinate triplet')
    x, y, z = (_spatial_number(value) for value in coordinate)
    radius = _spatial_number(data['radius'])
    if radius <= 0:
        raise ValueError('The radius must be positive')
    return TaskLink(name, match.group(1), (x, y, z), radius)


def _nearest_candidate(project_id, coordinate, radius, study_annotations, user, skipped_skeletons):
    x, y, z = coordinate
    with connection.cursor() as cursor:
        editable_user_ids = [] if user.is_superuser else list(user_domain(cursor, user.id))
        relations = get_relation_to_id_map(project_id, ['model_of', 'annotated_with'], cursor)
        classes = get_class_to_id_map(project_id, ['neuron', 'skeleton', 'annotation'], cursor)
        # The indexed edge bounds narrow the search before measuring distances to nodes.
        # Find each skeleton's closest node before checking neuron ownership.
        cursor.execute("""
            WITH nearby AS (
                SELECT DISTINCT ON (t.skeleton_id) t.skeleton_id, t.id AS treenode_id,
                    t.location_x, t.location_y, t.location_z,
                    ST_3DDistance(ST_StartPoint(e.edge), ST_MakePoint(%(x)s, %(y)s, %(z)s)) AS distance
                FROM treenode_edge e
                JOIN treenode t ON t.id = e.id AND t.project_id = e.project_id
                WHERE e.project_id = %(project)s
                  AND e.edge &&& ST_MakeLine(
                      ST_MakePoint(%(min_x)s, %(min_y)s, %(min_z)s),
                      ST_MakePoint(%(max_x)s, %(max_y)s, %(max_z)s))
                  AND ST_3DDWithin(ST_StartPoint(e.edge), ST_MakePoint(%(x)s, %(y)s, %(z)s), %(radius)s)
                ORDER BY t.skeleton_id, distance, t.id
            )
            SELECT candidate.skeleton_id, candidate.treenode_id,
                candidate.location_x, candidate.location_y, candidate.location_z
            FROM nearby candidate
            JOIN class_instance sk ON sk.id = candidate.skeleton_id
                AND sk.project_id = %(project)s AND sk.class_id = %(skeleton_class)s
            JOIN class_instance_class_instance model ON model.class_instance_a = sk.id
                AND model.project_id = %(project)s AND model.relation_id = %(model_of)s
            JOIN class_instance neuron ON neuron.id = model.class_instance_b
                AND neuron.project_id = %(project)s AND neuron.class_id = %(neuron_class)s
            WHERE NOT (sk.id = ANY(%(skipped_skeletons)s))
            AND NOT EXISTS (
                SELECT 1 FROM class_instance_class_instance other_model
                WHERE other_model.project_id = %(project)s AND other_model.relation_id = %(model_of)s
                  AND other_model.id <> model.id
                  AND (other_model.class_instance_a = sk.id OR other_model.class_instance_b = neuron.id)
            )
            AND NOT EXISTS (
                SELECT 1
                FROM class_instance_class_instance link
                JOIN class_instance label ON label.id = link.class_instance_b
                    AND label.project_id = %(project)s AND label.class_id = %(annotation_class)s
                WHERE link.project_id = %(project)s AND link.relation_id = %(annotated_with)s
                  AND link.class_instance_a = neuron.id AND label.name ~* %(study_annotations)s
            )
            AND (%(is_superuser)s OR NOT EXISTS (
                SELECT 1
                FROM class_instance_class_instance link
                JOIN class_instance label ON label.id = link.class_instance_b
                    AND label.project_id = %(project)s AND label.class_id = %(annotation_class)s
                WHERE link.project_id = %(project)s AND link.relation_id = %(annotated_with)s
                  AND link.class_instance_a = neuron.id AND label.name = 'locked'
                  AND NOT (link.user_id = ANY(%(editable_user_ids)s))
            ))
            ORDER BY candidate.distance, candidate.skeleton_id, candidate.treenode_id
            LIMIT 1
        """, {
            'project': project_id,
            'x': x, 'y': y, 'z': z, 'radius': radius,
            'min_x': x - radius, 'min_y': y - radius, 'min_z': z - radius,
            'max_x': x + radius, 'max_y': y + radius, 'max_z': z + radius,
            'model_of': relations['model_of'], 'annotated_with': relations['annotated_with'],
            'neuron_class': classes['neuron'], 'skeleton_class': classes['skeleton'],
            'annotation_class': classes['annotation'], 'study_annotations': study_annotations,
            'is_superuser': user.is_superuser, 'editable_user_ids': editable_user_ids,
            'skipped_skeletons': skipped_skeletons,
        })
        return cursor.fetchone()


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


def lock_task_annotation(project_id: int, name: str) -> None:
    """Serialize writes for a task whose annotation might not exist yet."""
    key = 'catmaid-task-link:%s:%s' % (project_id, name.lower())
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', [key])


def _neuron_annotation_links(project_id: int):
    return ClassInstanceClassInstance.objects.filter(
        project_id=project_id, relation__relation_name='annotated_with',
        class_instance_a__class_column__class_name='neuron',
        class_instance_b__project_id=project_id, class_instance_b__class_column__class_name='annotation',
    )


def _assign(project_id: int, user, change: TaskLink) -> int:
    lock_task_annotation(project_id, change.annotation)
    if _neuron_annotation_links(project_id).filter(class_instance_b__name__iexact=change.annotation).exists():
        raise TaskLinkConflict('The task already has a skeleton')
    study_annotations = r'^neuroglass:study:%s:task:%s$' % (change.study_id, UUID_PATTERN)
    skipped_skeletons = []
    while True:
        candidate = _nearest_candidate(project_id, change.coordinate, change.radius,
                study_annotations, user, skipped_skeletons)
        if candidate is None:
            raise TaskLinkNoMatch('No eligible skeleton within the requested radius')
        skeleton_id, treenode_id, x, y, z = candidate
        try:
            # Roll back any label and release this candidate's locks if its
            # sampler prevents rerooting before trying a different skeleton.
            with transaction.atomic():
                return _link_candidate(project_id, user, change, study_annotations,
                        skeleton_id, treenode_id, (x, y, z))
        except skeleton.RerootSamplerConflict:
            skipped_skeletons.append(skeleton_id)


def _link_candidate(project_id, user, change, study_annotations, skeleton_id, treenode_id,
        candidate_coordinate):
    skeleton._lock_skeletons_for_reroot(project_id, [skeleton_id])

    # Lock the neuron before the treenode, as whole-skeleton deletion does.
    # Single-node deletion is serialized by the topology lock above.
    neuron_id = _target_neuron(project_id, skeleton_id)
    if not ClassInstance.objects.select_for_update().filter(project_id=project_id, id=neuron_id).exists():
        raise TaskLinkConflict('The target neuron no longer exists')
    if _target_neuron(project_id, skeleton_id) != neuron_id:
        raise TaskLinkConflict('The target skeleton changed during assignment')

    try:
        locked_coordinate = Treenode.objects.select_for_update().values_list(
                'location_x', 'location_y', 'location_z').get(
                        project_id=project_id, id=treenode_id, skeleton_id=skeleton_id)
    except Treenode.DoesNotExist:
        raise TaskLinkConflict('The reroot node is no longer on the target skeleton')
    # Moving within the radius can also change which candidate is nearest.
    if (locked_coordinate != candidate_coordinate
            or math.dist(locked_coordinate, change.coordinate) > change.radius):
        raise TaskLinkConflict('The nearest node moved during assignment. Try starting again.')

    links = _neuron_annotation_links(project_id)
    if links.filter(class_instance_b__name__iexact=change.annotation).exists():
        raise TaskLinkConflict('The task already has a skeleton')
    if links.filter(class_instance_a_id=neuron_id, class_instance_b__name__iregex=study_annotations).exists():
        raise TaskLinkConflict('The skeleton already belongs to another task in this study')

    added, _, _ = annotation._annotate_entities(project_id,
            [neuron_id], {change.annotation: {'user_id': user.id}})
    if not any(neuron_id in entities for entities in added.values()):
        raise TaskLinkConflict('The task annotation could not be added')
    skeleton._reroot_skeleton_locked(treenode_id, project_id, skeleton_id)
    return skeleton_id


@api_view(['POST'])
@parser_classes([JSONParser])
@requires_user_role(UserRole.Annotate)
def link_task(request: HttpRequest, project_id=None) -> JsonResponse:
    """Assign an unlinked NeuroGlass task and reroot the skeleton atomically.

    Send one task annotation, an [x, y, z] coordinate and a radius in project nanometres.
    CATMAID chooses the closest eligible skeleton and makes its closest node the root.
    A successful response includes the assigned skeleton ID.
    """
    project_id = int(project_id)
    scope = getattr(request, '_neuroglass_task_link_scope', None)
    if scope is None or scope[0] != project_id:
        raise PermissionError('Task linking requires authorization for this project from NeuroGlass')
    try:
        change = _parse_link(request.data)
    except ValueError as error:
        return HttpResponseBadRequest(str(error))
    if change.study_id != scope[1]:
        raise PermissionError('The task annotation does not match the signed study')

    with transaction.atomic():
        skeleton_id = _assign(project_id, request.user, change)
    return JsonResponse({'skeleton_id': skeleton_id})
