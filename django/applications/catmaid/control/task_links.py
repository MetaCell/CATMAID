# -*- coding: utf-8 -*-

"""Atomically link NeuroGlass task annotations to skeletons."""

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Set

from django.db import connection, transaction
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse

from rest_framework.decorators import api_view

from catmaid.control import annotation, skeleton
from catmaid.control.authentication import PermissionError, requires_user_role
from catmaid.error import ClientError
from catmaid.models import Class, Relation, UserRole

UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TASK_ANNOTATION = re.compile(r'neuroglass:study:(%s):task:%s' % (UUID_PATTERN, UUID_PATTERN),
        re.IGNORECASE)
# The same pattern for PostgreSQL's case-insensitive ~* operator.
TASK_ANNOTATION_SQL = '^%s$' % TASK_ANNOTATION.pattern
OPERATION_KEYS = {'study_id', 'operation_id', 'deadline', 'operation_changes'}
MAX_DEADLINE = 253402300799
CHANGE_KEYS = {'annotation', 'expected_skeleton_id', 'skeleton_id', 'reroot_treenode_id'}


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


@dataclass
class OperationRequest:
    study_id: str
    deadline: int
    plans: List[ChangePlan]
    recover: bool


def _int_field(change: Dict, field: str, nullable=False) -> Optional[int]:
    value = change.get(field)
    if value is None and nullable:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError('"%s" must be an integer%s' % (
                field, ' or null' if nullable else ''))
    if not -(2**63) <= value < 2**63:
        raise ValueError('"%s" is outside the supported integer range' % field)
    return value


def _parse_changes(changes: Any, *, study_id: Optional[str] = None) -> List[ChangePlan]:
    """Return one plan per change or raise ValueError with a message."""
    if not isinstance(changes, list):
        field = 'operation_changes' if study_id is not None else 'changes'
        raise ValueError('"%s" must be a list' % field)
    plans = []
    for change in changes:
        if not isinstance(change, dict):
            raise ValueError('Every change must be an object')
        name = change.get('annotation')
        if not isinstance(name, str):
            raise ValueError('"annotation" must be a string')
        if study_id is not None:
            if change.keys() != CHANGE_KEYS:
                raise ValueError('Every operation change must have exactly the task-link fields')
            match = TASK_ANNOTATION.fullmatch(name)
            if not match or match.group(1).lower() != study_id:
                raise ValueError('Every operation annotation must be a task of the requested study')
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


def _canonical_uuid(data: Dict, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or re.fullmatch(UUID_PATTERN, value) is None:
        raise ValueError('"%s" must be a canonical UUID' % field)
    return value


def _parse_operation(data: Dict) -> OperationRequest:
    recover = data.keys() == OPERATION_KEYS | {'recover'} and data['recover'] is True
    if not recover and data.keys() != OPERATION_KEYS:
        raise ValueError('Expected a task-link apply or recovery request')
    study_id = _canonical_uuid(data, 'study_id')
    _canonical_uuid(data, 'operation_id')
    deadline = _int_field(data, 'deadline')
    if deadline is None or not 0 < deadline <= MAX_DEADLINE:
        raise ValueError('"deadline" must be an epoch second in the supported range')
    plans = _parse_changes(data['operation_changes'], study_id=study_id)
    if not plans:
        raise ValueError('"operation_changes" must not be empty')
    return OperationRequest(study_id, deadline, plans, recover)


def _schema_ids(project_id: int) -> Dict[str, int]:
    classes = dict(Class.objects.filter(project_id=project_id,
            class_name__in=('annotation', 'neuron', 'skeleton')).values_list('class_name', 'id'))
    relations = dict(Relation.objects.filter(project_id=project_id,
            relation_name__in=('annotated_with', 'model_of')).values_list('relation_name', 'id'))
    return {**classes, **relations, 'project_id': project_id}


def _validate(project_id: int, plans: List[ChangePlan]) -> List[ChangePlan]:
    """Check every change against the current state. Changes that can't be
    applied get a reason. Nothing is written. Can be run again on the same
    plans, e.g. once the involved rows are locked."""
    for plan in plans:
        plan.reason = plan.study_id = plan.annotation_id = None
        plan.previous_neuron_id = plan.target_neuron_id = None
    schema_ids = _schema_ids(project_id)
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
    ''', {**schema_ids,
          'names': [p.annotation for p in plans]})
    annotation_ids: Dict[str, int] = {}
    holders: Dict[str, List] = {}
    for name, annotation_id, neuron_id, skeleton_id in cursor.fetchall():
        annotation_ids[name] = annotation_id
        if neuron_id is not None:
            holders.setdefault(name, []).append((neuron_id, skeleton_id))

    # The destination must have one model_of relation in both directions:
    # task annotations belong to neurons, and a shared neuron would make a
    # successful link refer to more than one skeleton.
    cursor.execute('''
        SELECT s.id, neuron.id, (
            SELECT count(*)
            FROM class_instance_class_instance reverse_model
            WHERE reverse_model.project_id = %(project_id)s
              AND reverse_model.relation_id = %(model_of)s
              AND reverse_model.class_instance_b = neuron.id
        )
        FROM class_instance s
        LEFT JOIN class_instance_class_instance model
            ON model.class_instance_a = s.id AND model.relation_id = %(model_of)s
            AND model.project_id = %(project_id)s
        LEFT JOIN class_instance neuron
            ON neuron.id = model.class_instance_b AND neuron.class_id = %(neuron)s
            AND neuron.project_id = %(project_id)s
        WHERE s.project_id = %(project_id)s AND s.class_id = %(skeleton)s
          AND s.id = ANY(%(skeleton_ids)s::bigint[])
    ''', {**schema_ids,
          'skeleton_ids': [p.skeleton_id for p in plans]})
    target_neurons: Dict[int, List] = {}
    for skeleton_id, neuron_id, model_count in cursor.fetchall():
        target_neurons.setdefault(skeleton_id, []).append((neuron_id, model_count))

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
        models = target_neurons[plan.skeleton_id]
        if len(models) != 1 or models[0][0] is None:
            plan.reason = 'Skeleton %s is not modeling exactly one neuron' % plan.skeleton_id
            continue
        neuron_id, model_count = models[0]
        if model_count != 1:
            plan.reason = 'The target neuron is not modeled by exactly one skeleton'
            continue
        plan.target_neuron_id = neuron_id
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
    ''', {**schema_ids,
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


def _lock_task_annotations(project_id: int, plans: List[ChangePlan]) -> None:
    """Serialize independent operations even if the task has no holder yet."""
    names = ['catmaid-task-link:%s:%s' % (project_id, p.annotation.lower())
            for p in plans if TASK_ANNOTATION.fullmatch(p.annotation)]
    with connection.cursor() as cursor:
        cursor.execute('''
            SELECT pg_advisory_xact_lock(lock_id)
            FROM (
                SELECT DISTINCT hashtextextended(name, 0) AS lock_id
                FROM unnest(%s::text[]) AS names(name)
                ORDER BY lock_id
            ) locks
        ''', [names])


def _validate_locked(project_id: int, plans: List[ChangePlan]) -> List[ChangePlan]:
    """Lock tasks and reroot targets before checking the current holder.
    Like NeuroGlass' deletion guard, acquire treenode row locks before neuron
    row locks. Must run inside a transaction."""
    _lock_task_annotations(project_id, plans)
    skeleton._lock_skeletons_for_reroot(project_id, [p.skeleton_id for p in plans])
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


def _apply_plans(project_id: int, user, plans: List[ChangePlan]) -> List[Dict]:
    _validate_locked(project_id, plans)
    _apply(project_id, user, plans)
    return [{
        'annotation': p.annotation,
        'status': 'applied' if p.applied else 'skipped',
        'reason': p.reason,
        'rerooted': p.rerooted,
    } for p in plans]


def _observe_task_links(project_id: int, plans: List[ChangePlan]) -> List[Dict]:
    # Read links and roots in one statement so they share the same snapshot.
    # An unlinked task or an ambiguous set of links has no current skeleton.
    with connection.cursor() as cursor:
        cursor.execute('''
            WITH requested(name, ordinal) AS (
                SELECT * FROM unnest(%(names)s::text[]) WITH ORDINALITY
            ), linked AS (
                SELECT requested.ordinal, MIN(skeleton.id) AS skeleton_id
                FROM requested
                JOIN class_instance a ON a.project_id = %(project_id)s
                    AND a.class_id = %(annotation)s AND a.name = requested.name
                JOIN class_instance_class_instance link ON link.class_instance_b = a.id
                    AND link.project_id = %(project_id)s AND link.relation_id = %(annotated_with)s
                JOIN class_instance neuron ON neuron.id = link.class_instance_a
                    AND neuron.project_id = %(project_id)s AND neuron.class_id = %(neuron)s
                LEFT JOIN class_instance_class_instance model ON model.class_instance_b = neuron.id
                    AND model.project_id = %(project_id)s AND model.relation_id = %(model_of)s
                LEFT JOIN class_instance skeleton ON skeleton.id = model.class_instance_a
                    AND skeleton.project_id = %(project_id)s AND skeleton.class_id = %(skeleton)s
                GROUP BY requested.ordinal
                HAVING COUNT(*) = 1
            )
            SELECT requested.name, linked.skeleton_id, (
                SELECT MIN(t.id)
                FROM treenode t
                WHERE t.project_id = %(project_id)s AND t.skeleton_id = linked.skeleton_id
                    AND t.parent_id IS NULL
                HAVING COUNT(*) = 1
            ) AS root_id
            FROM requested
            LEFT JOIN linked ON linked.ordinal = requested.ordinal
            ORDER BY requested.ordinal
        ''', {**_schema_ids(project_id), 'names': [p.annotation for p in plans]})
        return [{'annotation': name, 'skeleton_id': skeleton_id, 'root_id': root_id}
                for name, skeleton_id, root_id in cursor.fetchall()]


def _resolve_operation(project_id: int, user, operation: OperationRequest) -> Dict:
    # Recovery waits for admitted operations to commit or roll back. Read the
    # database wall clock after acquiring the lock, including time spent waiting.
    with connection.cursor() as cursor:
        lock_key = 'catmaid-task-link-study:%s:%s' % (project_id, operation.study_id)
        cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', [lock_key])
        cursor.execute('SELECT clock_timestamp() >= to_timestamp(%s)', [operation.deadline])
        expired = cursor.fetchone()[0]

    if operation.recover:
        if not expired:
            return {'state': 'pending', 'results': []}
        return {'state': 'observed', 'results': _observe_task_links(project_id, operation.plans)}
    if expired:
        return {'state': 'expired', 'results': []}
    return {'state': 'completed', 'results': _apply_plans(project_id, user, operation.plans)}


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

    The legacy changes request returns results. A deadline-bound apply instead
    supplies study_id, operation_id, deadline (Unix epoch seconds), and
    operation_changes. It returns state completed or expired and results.
    Recovery sends the same fields and recover set to true. It returns pending
    before the deadline or observed with one current skeleton and root per
    requested annotation once admitted writes have finished.
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
    # Only the NeuroGlass policy may grant this capability on the request,
    # after verifying the signed project, study and payload. Native CATMAID
    # annotation permissions alone must never activate this workflow.
    if getattr(request, '_neuroglass_task_link_authorized', False) is not True:
        raise PermissionError('Task links require authorization from NeuroGlass')

    data = request.data
    try:
        if not isinstance(data, dict):
            raise ValueError('Expected a task-link request object')
        operation = None
        if data.keys() == {'changes'}:
            plans = _parse_changes(data['changes'])
        else:
            operation = _parse_operation(data)
    except ValueError as e:
        return HttpResponseBadRequest(str(e))

    project_id = int(project_id)
    with transaction.atomic():
        if operation is None:
            reply = {'results': _apply_plans(project_id, request.user, plans)}
        else:
            reply = _resolve_operation(project_id, request.user, operation)
    return JsonResponse(reply)
