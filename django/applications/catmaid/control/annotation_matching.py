# -*- coding: utf-8 -*-

"""Annotate and reroot the closest eligible skeleton in one transaction."""

from dataclasses import dataclass
import math
import re
import sys

from django.db import DataError, connection, transaction
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser

from catmaid.control import annotation, skeleton
from catmaid.control.authentication import can_edit_class_instance_or_fail, requires_user_role, user_domain
from catmaid.control.common import get_class_to_id_map, get_relation_to_id_map
from catmaid.error import ClientError
from catmaid.locks import lock_skeleton_topology, skeleton_edit_lock_timeout
from catmaid.models import ClassInstance, ClassInstanceClassInstance, Treenode, UserRole

# Leave room for the search bounds and squared 3D distances in double precision.
MAX_SPATIAL_MAGNITUDE = math.sqrt(sys.float_info.max) / 4


class AnnotationMatchConflict(ClientError):
    status_code = 409


class AnnotationMatchNoMatch(AnnotationMatchConflict):
    meta = {'code': 'annotation_match_no_match'}


@dataclass(frozen=True)
class NearestAnnotation:
    annotation: str
    exclude_annotation_pattern: str | None
    coordinate: tuple[float, float, float]
    radius: float


def _spatial_number(value) -> float:
    if type(value) not in (int, float) or not -MAX_SPATIAL_MAGNITUDE <= value <= MAX_SPATIAL_MAGNITUDE:
        raise ValueError('Expected a finite number in the supported spatial range')
    return float(value)


def _parse_request(data) -> NearestAnnotation:
    if (not isinstance(data, dict) or not {'annotation', 'coordinate', 'radius'} <= data.keys()
            or data.keys() - {'annotation', 'coordinate', 'radius', 'exclude_annotation_pattern'}):
        raise ValueError('Expected an annotation, one coordinate and a radius')
    name = data['annotation']
    if not isinstance(name, str) or not name.strip() or '\x00' in name:
        raise ValueError('Expected a nonempty annotation name')
    max_name_length = ClassInstance._meta.get_field('name').max_length
    if len(name) > max_name_length:
        raise ValueError(f'The annotation name cannot exceed {max_name_length} characters')
    if re.search(r'\{n(?:\d+)?\}', name):
        raise ValueError('Annotation counting patterns are not supported')
    pattern = data.get('exclude_annotation_pattern')
    if pattern is not None:
        if not isinstance(pattern, str):
            raise ValueError('Expected an annotation exclusion pattern')
        try:
            with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute("SELECT '' ~* %s", [pattern])
        except DataError as error:
            raise ValueError('Invalid annotation exclusion pattern') from error
    coordinate = data['coordinate']
    if not isinstance(coordinate, list) or len(coordinate) != 3:
        raise ValueError('Expected one [x, y, z] coordinate triplet')
    x, y, z = (_spatial_number(value) for value in coordinate)
    radius = _spatial_number(data['radius'])
    if radius <= 0:
        raise ValueError('The radius must be positive')
    return NearestAnnotation(name, pattern, (x, y, z), radius)


def _nearest_candidate(project_id, coordinate, radius, exclude_annotation_pattern, user):
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
            SELECT candidate.skeleton_id, candidate.treenode_id, neuron.id,
                candidate.location_x, candidate.location_y, candidate.location_z
            FROM nearby candidate
            JOIN class_instance sk ON sk.id = candidate.skeleton_id
                AND sk.project_id = %(project)s AND sk.class_id = %(skeleton_class)s
            JOIN class_instance_class_instance model ON model.class_instance_a = sk.id
                AND model.project_id = %(project)s AND model.relation_id = %(model_of)s
            JOIN class_instance neuron ON neuron.id = model.class_instance_b
                AND neuron.project_id = %(project)s AND neuron.class_id = %(neuron_class)s
            WHERE NOT EXISTS (
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
                  AND link.class_instance_a = neuron.id AND label.name ~* %(exclude_annotation_pattern)s
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
            'annotation_class': classes['annotation'], 'exclude_annotation_pattern': exclude_annotation_pattern,
            'is_superuser': user.is_superuser, 'editable_user_ids': editable_user_ids,
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
        raise AnnotationMatchConflict('The target skeleton must model exactly one neuron')
    if models.filter(class_instance_b_id=targets[0]).exclude(class_instance_a_id=skeleton_id).exists():
        raise AnnotationMatchConflict('The target neuron must be modeled by exactly one skeleton')
    return targets[0]


def lock_annotation(project_id: int, name: str) -> None:
    """Serialize assignment of an annotation that might not exist yet."""
    key = 'catmaid-annotation-match:%s:%s' % (project_id, name.lower())
    with connection.cursor() as cursor, skeleton_edit_lock_timeout(cursor):
        cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', [key])


def _neuron_annotation_links(project_id: int):
    return ClassInstanceClassInstance.objects.filter(
        project_id=project_id, relation__relation_name='annotated_with',
        class_instance_a__class_column__class_name='neuron',
        class_instance_b__project_id=project_id, class_instance_b__class_column__class_name='annotation',
    )


def _assign(project_id: int, user, change: NearestAnnotation) -> tuple[int, int]:
    lock_annotation(project_id, change.annotation)
    if _neuron_annotation_links(project_id).filter(class_instance_b__name__iexact=change.annotation).exists():
        raise AnnotationMatchConflict('The annotation already belongs to a skeleton')
    candidate = _nearest_candidate(project_id, change.coordinate, change.radius,
            change.exclude_annotation_pattern, user)
    if candidate is None:
        raise AnnotationMatchNoMatch('No eligible skeleton within the requested radius')
    skeleton_id, treenode_id, neuron_id, x, y, z = candidate
    _link_candidate(project_id, user, change,
            skeleton_id, treenode_id, neuron_id, (x, y, z))
    return skeleton_id, treenode_id


def _link_candidate(project_id, user, change, skeleton_id, treenode_id,
        neuron_id, candidate_coordinate):
    lock_skeleton_topology(project_id, [skeleton_id])

    # Lock the neuron before the treenode, as whole-skeleton deletion does.
    # Single-node deletion is serialized by the topology lock above.
    try:
        with connection.cursor() as cursor, skeleton_edit_lock_timeout(cursor):
            if not ClassInstance.objects.select_for_update().filter(
                    project_id=project_id, id=neuron_id).exists():
                raise AnnotationMatchConflict('The target neuron no longer exists')
            if _target_neuron(project_id, skeleton_id) != neuron_id:
                raise AnnotationMatchConflict('The target skeleton changed during assignment')

            # Block edits to the node while allowing foreign key references to it.
            locked_coordinate = Treenode.objects.select_for_update(no_key=True).values_list(
                    'location_x', 'location_y', 'location_z').get(
                            project_id=project_id, id=treenode_id, skeleton_id=skeleton_id)
    except Treenode.DoesNotExist:
        raise AnnotationMatchConflict('The reroot node is no longer on the target skeleton')
    # Moving within the radius can also change which candidate is nearest.
    if locked_coordinate != candidate_coordinate:
        raise AnnotationMatchConflict('The nearest node moved during annotation. Retry the request.')

    links = _neuron_annotation_links(project_id)
    if links.filter(class_instance_b__name__iexact=change.annotation).exists():
        raise AnnotationMatchConflict('The annotation already belongs to a skeleton')
    if (change.exclude_annotation_pattern is not None and links.filter(
            class_instance_a_id=neuron_id,
            class_instance_b__name__iregex=change.exclude_annotation_pattern).exists()):
        raise AnnotationMatchConflict('The skeleton carries an excluded annotation')

    can_edit_class_instance_or_fail(user, neuron_id, 'neuron')
    added, _, _ = annotation._annotate_entities(project_id,
            [neuron_id], {change.annotation: {'user_id': user.id}})
    if not any(neuron_id in entities for entities in added.values()):
        raise AnnotationMatchConflict('The annotation could not be added')
    skeleton._reroot_skeleton_locked(treenode_id, project_id, skeleton_id)


@api_view(['POST'])
@parser_classes([JSONParser])
@requires_user_role(UserRole.Annotate)
def annotate_nearest(request: HttpRequest, project_id=None) -> JsonResponse:
    """Annotate the nearest eligible skeleton and reroot it atomically.

    Send an annotation, an [x, y, z] coordinate and a positive radius in project
    nanometres. An optional exclude_annotation_pattern skips neurons carrying
    matching annotations (case-insensitive PostgreSQL regular expression).
    The annotation must be unused on neurons in this project. The response
    contains the selected skeleton_id and root_id.
    """
    project_id = int(project_id)
    try:
        change = _parse_request(request.data)
    except ValueError as error:
        return HttpResponseBadRequest(str(error))

    with transaction.atomic():
        skeleton_id, root_id = _assign(project_id, request.user, change)
    return JsonResponse({'skeleton_id': skeleton_id, 'root_id': root_id})
