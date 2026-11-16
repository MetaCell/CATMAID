# -*- coding: utf-8 -*-

"""Atomically link NeuroGlass task annotations to skeletons."""

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Set

from django.db import connection, transaction
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse

from rest_framework.decorators import api_view

from catmaid.control import annotation, skeleton
from catmaid.control.authentication import requires_user_role
from catmaid.error import ClientError
from catmaid.models import Class, Relation, UserRole

UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TASK_ANNOTATION = re.compile(r'neuroglass:study:(%s):task:%s' % (UUID_PATTERN, UUID_PATTERN),
        re.IGNORECASE)
# The same pattern for PostgreSQL's case-insensitive ~* operator.
TASK_ANNOTATION_SQL = '^%s$' % TASK_ANNOTATION.pattern


class TaskLinkConflict(ClientError):
    """A validated change could not be fully applied."""
    status_code = 409


@dataclass
class ChangePlan:
    annotation: str
    expected_skeleton_id: Optional[int]
    skeleton_id: int
    reroot_treenode_id: int
    reason: Optional[str] = None
    study_id: Optional[str] = None
    annotation_id: Optional[int] = None
    previous_neuron_id: Optional[int] = None
    target_neuron_id: Optional[int] = None
    rerooted: bool = False

    @property
    def applied(self) -> bool:
        return self.reason is None


def _int_field(change: Dict, field: str, nullable=False) -> Optional[int]:
    value = change.get(field)
    if value is None and nullable:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError('"%s" must be an integer%s' % (
                field, ' or null' if nullable else ''))
    return value


def _parse_request(data: Any) -> List[ChangePlan]:
    """Return one plan per change or raise ValueError with a message."""
    changes = data.get('changes') if isinstance(data, dict) else None
    if not isinstance(changes, list):
        raise ValueError('"changes" must be a list')
    plans = []
    for change in changes:
        if not isinstance(change, dict):
            raise ValueError('Every change must be an object')
        name = change.get('annotation')
        if not isinstance(name, str):
            raise ValueError('"annotation" must be a string')
        if 'expected_skeleton_id' not in change:
            raise ValueError('"expected_skeleton_id" is required')
        plans.append(ChangePlan(
            annotation=name,
            expected_skeleton_id=_int_field(change, 'expected_skeleton_id', nullable=True),
            skeleton_id=_int_field(change, 'skeleton_id'),
            reroot_treenode_id=_int_field(change, 'reroot_treenode_id')))
    names = [p.annotation for p in plans]
    if len(set(names)) != len(names):
        raise ValueError('Every annotation can only be changed once per request')
    skeleton_ids = [p.skeleton_id for p in plans]
    if len(set(skeleton_ids)) != len(skeleton_ids):
        raise ValueError('Every skeleton can only be targeted once per request')
    return plans


def _validate(project_id: int, plans: List[ChangePlan]) -> List[ChangePlan]:
    """Check every change against the current state. Changes that can't be
    applied get a reason. Nothing is written. Can be run again on the same
    plans, e.g. once the involved rows are locked."""
    for plan in plans:
        plan.reason = plan.study_id = plan.annotation_id = None
        plan.previous_neuron_id = plan.target_neuron_id = None
    classes = dict(Class.objects.filter(project_id=project_id,
            class_name__in=('annotation', 'neuron', 'skeleton')).values_list('class_name', 'id'))
    relations = dict(Relation.objects.filter(project_id=project_id,
            relation_name__in=('annotated_with', 'model_of')).values_list('relation_name', 'id'))
    cursor = connection.cursor()

    for plan in plans:
        match = TASK_ANNOTATION.fullmatch(plan.annotation)
        if match:
            plan.study_id = match.group(1).lower()
        else:
            plan.reason = 'The annotation is not a task annotation'

    # Where every annotation is now: neuron and skeleton per annotation link.
    cursor.execute('''
        SELECT a.name, a.id, link.class_instance_a, model.class_instance_a
        FROM class_instance a
        LEFT JOIN (class_instance_class_instance link
            JOIN class_instance neuron
                ON neuron.id = link.class_instance_a AND neuron.class_id = %(neuron)s)
            ON link.class_instance_b = a.id AND link.relation_id = %(annotated_with)s
        LEFT JOIN class_instance_class_instance model
            ON model.class_instance_b = neuron.id AND model.relation_id = %(model_of)s
        WHERE a.project_id = %(project_id)s AND a.class_id = %(annotation)s
          AND a.name = ANY(%(names)s::text[])
    ''', {**classes, **relations, 'project_id': project_id,
          'names': [p.annotation for p in plans]})
    annotation_ids: Dict[str, int] = {}
    holders: Dict[str, List] = {}
    for name, annotation_id, neuron_id, skeleton_id in cursor.fetchall():
        annotation_ids[name] = annotation_id
        if neuron_id is not None:
            holders.setdefault(name, []).append((neuron_id, skeleton_id))

    # The neuron of every target skeleton.
    cursor.execute('''
        SELECT s.id, model.class_instance_b
        FROM class_instance s
        LEFT JOIN class_instance_class_instance model
            ON model.class_instance_a = s.id AND model.relation_id = %(model_of)s
        WHERE s.project_id = %(project_id)s AND s.class_id = %(skeleton)s
          AND s.id = ANY(%(skeleton_ids)s::bigint[])
    ''', {**classes, **relations, 'project_id': project_id,
          'skeleton_ids': [p.skeleton_id for p in plans]})
    target_neurons: Dict[int, List[int]] = {}
    for skeleton_id, neuron_id in cursor.fetchall():
        target_neurons.setdefault(skeleton_id, [])
        if neuron_id is not None:
            target_neurons[skeleton_id].append(neuron_id)

    cursor.execute('''
        SELECT id, skeleton_id FROM treenode
        WHERE project_id = %s AND id = ANY(%s::bigint[])
    ''', [project_id, [p.reroot_treenode_id for p in plans]])
    node_skeletons = dict(cursor.fetchall())

    for plan in plans:
        if not plan.applied:
            continue
        plan.annotation_id = annotation_ids.get(plan.annotation)
        current = holders.get(plan.annotation, [])
        if len(current) > 1 or (current and current[0][1] is None):
            plan.reason = 'The annotation is not linked to exactly one skeleton'
            continue
        current_skeleton_id = current[0][1] if current else None
        if current_skeleton_id != plan.expected_skeleton_id:
            plan.reason = 'The annotation is linked to skeleton %s, not %s' % (
                    current_skeleton_id, plan.expected_skeleton_id)
            continue
        plan.previous_neuron_id = current[0][0] if current else None
        if plan.skeleton_id not in target_neurons:
            plan.reason = 'Skeleton %s does not exist' % plan.skeleton_id
            continue
        if len(target_neurons[plan.skeleton_id]) != 1:
            plan.reason = 'Skeleton %s is not modeling exactly one neuron' % plan.skeleton_id
            continue
        plan.target_neuron_id = target_neurons[plan.skeleton_id][0]
        if node_skeletons.get(plan.reroot_treenode_id) != plan.skeleton_id:
            plan.reason = 'Node %s is not part of skeleton %s' % (
                    plan.reroot_treenode_id, plan.skeleton_id)

    # Task annotations already on the target neurons.
    target_ids = [p.target_neuron_id for p in plans if p.applied]
    cursor.execute('''
        SELECT link.class_instance_a, a.name
        FROM class_instance_class_instance link
        JOIN class_instance a ON a.id = link.class_instance_b
        WHERE link.project_id = %(project_id)s AND link.relation_id = %(annotated_with)s
          AND link.class_instance_a = ANY(%(target_ids)s::bigint[])
          AND a.class_id = %(annotation)s AND a.name ~* %(pattern)s
    ''', {**classes, **relations, 'project_id': project_id,
          'target_ids': target_ids, 'pattern': TASK_ANNOTATION_SQL})
    tasks_on_neuron: Dict[int, List[str]] = {}
    for neuron_id, name in cursor.fetchall():
        tasks_on_neuron.setdefault(neuron_id, []).append(name)

    # A target may hold another task of the same study only if that task is
    # moved off it by an applied change. Skipping one change can invalidate
    # others, so repeat until nothing changes.
    while True:
        moved_off = {(p.previous_neuron_id, p.annotation) for p in plans
                if p.applied and p.previous_neuron_id not in (None, p.target_neuron_id)}
        newly_skipped = False
        for plan in plans:
            if not plan.applied:
                continue
            for name in tasks_on_neuron.get(plan.target_neuron_id, []):
                if name.lower() == plan.annotation.lower():
                    continue
                if TASK_ANNOTATION.fullmatch(name).group(1).lower() != plan.study_id:
                    continue
                if (plan.target_neuron_id, name) in moved_off:
                    continue
                plan.reason = 'Skeleton %s is linked to task annotation %s' % (
                        plan.skeleton_id, name)
                newly_skipped = True
                break
        if not newly_skipped:
            return plans


def _neuron_ids(plans: List[ChangePlan]) -> Set[int]:
    """The target and previous neurons of all applied changes."""
    applied = [p for p in plans if p.applied]
    return {p.target_neuron_id for p in applied} | \
            {p.previous_neuron_id for p in applied if p.previous_neuron_id is not None}


def _lock_treenodes(project_id: int, node_ids) -> None:
    """Lock the reroot nodes in ID order. Missing nodes fail validation."""
    cursor = connection.cursor()
    cursor.execute('''
        SELECT id FROM treenode
        WHERE project_id = %s AND id = ANY(%s::bigint[])
        ORDER BY id FOR UPDATE
    ''', [project_id, sorted(set(node_ids))])


def _lock_neurons(project_id: int, neuron_ids) -> None:
    """Take the same row locks as NeuroGlass' task guard."""
    neuron_ids = sorted(set(neuron_ids))
    if not neuron_ids:
        return
    cursor = connection.cursor()
    cursor.execute('''
        SELECT id FROM class_instance
        WHERE project_id = %s AND id = ANY(%s::bigint[])
        ORDER BY id FOR UPDATE
    ''', [project_id, neuron_ids])
    if [row[0] for row in cursor.fetchall()] != neuron_ids:
        raise TaskLinkConflict('A neuron to link no longer exists, please retry')


def _validate_locked(project_id: int, plans: List[ChangePlan]) -> List[ChangePlan]:
    """Validate the plans against locked state. Like NeuroGlass' task skeleton
    deletion guard, treenode rows are locked before neuron rows. The neurons to
    lock come from a first validation; the second one runs under the locks.
    Must run inside a transaction."""
    _lock_treenodes(project_id, [p.reroot_treenode_id for p in plans])
    locked = _neuron_ids(_validate(project_id, plans))
    _lock_neurons(project_id, locked)
    _validate(project_id, plans)
    if not _neuron_ids(plans) <= locked:
        raise TaskLinkConflict('Task links changed during the request, please retry')
    return plans


def _apply(project_id: int, user, plans: List[ChangePlan]) -> None:
    """Write all applied changes. Must run inside a transaction, after
    _validate_locked: any exception leaves every change unwritten."""
    for plan in (p for p in plans if p.applied):
        if plan.target_neuron_id != plan.previous_neuron_id:
            # Add before removing, so that the annotation is never unused and
            # therefore kept with its ID and meta-annotations. Both helpers
            # are called as module attributes, so that wrappers installed by
            # NeuroGlass apply.
            annotated, _, _ = annotation._annotate_entities(project_id,
                    [plan.target_neuron_id], {plan.annotation: {'user_id': user.id}})
            if not any(plan.target_neuron_id in entities for entities in annotated.values()):
                raise TaskLinkConflict('Annotation %s could not be added to skeleton %s' % (
                        plan.annotation, plan.skeleton_id))
            if plan.previous_neuron_id is not None:
                deleted, missed, _, _ = annotation._remove_annotation(user, project_id,
                        [plan.previous_neuron_id], plan.annotation_id)
                if missed or len(deleted) != 1:
                    raise TaskLinkConflict('Annotation %s could not be removed from skeleton %s' % (
                            plan.annotation, plan.expected_skeleton_id))
        # The reroot node is locked by _validate_locked.
        plan.rerooted = bool(skeleton._reroot_skeleton(plan.reroot_treenode_id, project_id))


@api_view(['POST'])
@requires_user_role(UserRole.Annotate)
def link_tasks(request: HttpRequest, project_id=None) -> JsonResponse:
    """Link NeuroGlass task annotations to skeletons and reroot them, all in
    one transaction.

    Every change moves a task annotation from the neuron of its current
    skeleton (if any) to the neuron of a new skeleton, which is then rerooted
    at the given node. All changes are validated against the current state
    first. Changes that fail validation are skipped and reported with a reason.
    All other changes are applied together: if any of them fails, none is
    written and an error is returned.
    ---
    parameters:
    - name: project_id
      description: Project to work in
      required: true
      type: integer
      paramType: path
    - name: changes
      description: |
        A list of objects with the fields annotation (a task annotation name
        neuroglass:study:<uuid>:task:<uuid>), expected_skeleton_id (the
        skeleton the annotation is linked to now, or null if none),
        skeleton_id (the skeleton to link it to) and reroot_treenode_id (a
        node of skeleton_id to become its root). Every annotation and every
        skeleton_id can only be used once.
      required: true
      type: array
      paramType: form
    type:
      results:
        type: array
        description: |
          One entry per change, in input order, with the fields annotation,
          status ("applied" or "skipped"), reason (why it was skipped, or
          null) and rerooted (false if the node was root already or the change
          was skipped).
        required: true
    """
    try:
        plans = _parse_request(request.data)
    except ValueError as e:
        return HttpResponseBadRequest(str(e))

    project_id = int(project_id)
    with transaction.atomic():
        _validate_locked(project_id, plans)
        _apply(project_id, request.user, plans)

    return JsonResponse({'results': [{
        'annotation': p.annotation,
        'status': 'applied' if p.applied else 'skipped',
        'reason': p.reason,
        'rerooted': p.rerooted,
    } for p in plans]})
