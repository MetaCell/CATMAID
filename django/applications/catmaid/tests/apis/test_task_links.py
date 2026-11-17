# -*- coding: utf-8 -*-

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from time import monotonic, sleep
from unittest.mock import patch

from django.db import connection, connections
from django.test import Client
from django.test.client import ClientHandler

from catmaid.control import annotation, task_links
from catmaid.models import Class, ClassInstance, ClassInstanceClassInstance, Treenode

from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


SKELETON_1_NEURON = 2
SKELETON_373_NEURON = 374


def task_annotation(study_id):
    return 'neuroglass:study:%s:task:%s' % (study_id, uuid.uuid4())


class TrustedTaskLinkHandler(ClientHandler):
    def get_response(self, request):
        # Stand in for the installed overlay's server-side grant.
        request._neuroglass_task_link_authorized = True
        return super().get_response(request)


class TaskLinkTestClient(Client):
    def __init__(self):
        super().__init__()
        self.handler = TrustedTaskLinkHandler(enforce_csrf_checks=False)


class TaskLinksFixtures:
    def setUp(self):
        super().setUp()
        self.client = TaskLinkTestClient()
        self.url = '/%d/skeletons/task-links' % self.test_project_id
        self.study_id = uuid.uuid4()
        self.task_a = task_annotation(self.study_id)
        self.task_b = task_annotation(self.study_id)
        self.fake_authentication()

    def link(self, name, neuron_id):
        annotation._annotate_entities(self.test_project_id, [neuron_id],
                {name: {'user_id': self.test_user_id}})

    def neurons_with(self, name):
        return set(ClassInstanceClassInstance.objects.filter(
                project_id=self.test_project_id,
                relation__relation_name='annotated_with',
                class_instance_b__name=name).values_list(
                        'class_instance_a_id', flat=True))

    def root(self, skeleton_id):
        response = self.client.get('/%d/skeletons/%d/root' % (
                self.test_project_id, skeleton_id))
        self.assertStatus(response)
        return json.loads(response.content.decode('utf-8'))['root_id']

    def change(self, name, expected, skeleton_id, reroot):
        return {
            'annotation': name,
            'expected_skeleton_id': expected,
            'skeleton_id': skeleton_id,
            'reroot_treenode_id': reroot,
        }

    def post(self, *changes):
        return self.client.post(self.url, json.dumps({'changes': list(changes)}),
                content_type='application/json')

    def results(self, response):
        self.assertStatus(response)
        body = response.json()
        self.assertEqual({'results'}, body.keys())
        return body['results']

    def post_operation(self, payload, client=None):
        return (client or self.client).post(self.url, payload,
                content_type='application/json')

    def operation(self, **kwargs):
        return {'study_id': str(self.study_id), 'operation_id': str(uuid.uuid4()),
                'deadline': 253402300799, **kwargs}

    def recovery(self, apply):
        return {**{key: apply[key] for key in ('study_id', 'operation_id', 'deadline')},
                'recover': True}


class TaskLinksApiTests(TaskLinksFixtures, CatmaidApiTestCase):
    def test_native_annotator_cannot_apply_task_links(self):
        native = Client()
        native.cookies.update(self.client.cookies)
        apply = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        self.assertEqual(403, self.post_operation(apply, native).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_target_neuron_with_two_skeletons_is_skipped(self):
        ClassInstanceClassInstance.objects.filter(
                project_id=self.test_project_id, relation__relation_name='model_of',
                class_instance_a_id=373).update(class_instance_b_id=SKELETON_1_NEURON)
        results = self.results(self.post(self.change(self.task_a, None, 1, 11)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_bigint_overflow_is_rejected(self):
        change = self.change(self.task_a, None, 1, 11)
        for field in ('skeleton_id', 'expected_skeleton_id', 'reroot_treenode_id'):
            with self.subTest(field=field):
                self.assertEqual(400, self.post(dict(change, **{field: 2**63})).status_code)

    def test_links_unlinked_task_and_reroots(self):
        results = self.results(self.post(self.change(self.task_a, None, 1, 11)))
        self.assertEqual([{'annotation': self.task_a, 'status': 'applied',
                'reason': None, 'rerooted': True}], results)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_relink_moves_annotation(self):
        self.link(self.task_a, SKELETON_373_NEURON)
        annotation_ids = set(ClassInstanceClassInstance.objects.filter(
                class_instance_b__name=self.task_a).values_list(
                        'class_instance_b_id', flat=True))
        results = self.results(self.post(self.change(self.task_a, 373, 1, 11)))
        self.assertEqual('applied', results[0]['status'])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        # The annotation itself is kept, not deleted and recreated.
        self.assertEqual(annotation_ids, set(ClassInstanceClassInstance.objects.filter(
                class_instance_b__name=self.task_a).values_list(
                        'class_instance_b_id', flat=True)))

    def test_same_skeleton_keeps_annotation_and_reroots(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        results = self.results(self.post(self.change(self.task_a, 1, 1, 11)))
        self.assertEqual([{'annotation': self.task_a, 'status': 'applied',
                'reason': None, 'rerooted': True}], results)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_chain_in_one_request(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        self.link(self.task_b, SKELETON_373_NEURON)
        results = self.results(self.post(
                self.change(self.task_a, 1, 373, 405),
                self.change(self.task_b, 373, 1, 11)))
        self.assertEqual(['applied', 'applied'], [r['status'] for r in results])
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_b))
        self.assertEqual(405, self.root(373))
        self.assertEqual(11, self.root(1))

    def test_stale_expectation_is_skipped(self):
        self.link(self.task_a, SKELETON_373_NEURON)
        results = self.results(self.post(self.change(self.task_a, 1, 1, 11)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertTrue(results[0]['reason'])
        self.assertFalse(results[0]['rerooted'])
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_target_held_by_other_task_is_skipped(self):
        self.link(self.task_b, SKELETON_1_NEURON)
        results = self.results(self.post(self.change(self.task_a, None, 1, 11)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertTrue(results[0]['reason'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_b))
        self.assertEqual(7, self.root(1))

    def test_target_held_by_task_of_other_study_is_applied(self):
        other = task_annotation(uuid.uuid4())
        self.link(other, SKELETON_1_NEURON)
        results = self.results(self.post(self.change(self.task_a, None, 1, 11)))
        self.assertEqual('applied', results[0]['status'])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))

    def test_reroot_node_not_in_skeleton_is_skipped(self):
        results = self.results(self.post(self.change(self.task_a, None, 1, 405)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertTrue(results[0]['reason'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(377, self.root(373))

    def test_non_task_annotation_is_skipped(self):
        results = self.results(self.post(self.change('my annotation', None, 1, 11)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertEqual(set(), self.neurons_with('my annotation'))
        self.assertEqual(7, self.root(1))

    def test_failure_mid_apply_rolls_back_everything(self):
        original = annotation._annotate_entities
        calls = []

        def fail_second(*args, **kwargs):
            calls.append(args)
            if len(calls) == 2:
                raise ValueError('Simulated failure')
            return original(*args, **kwargs)

        with patch.object(annotation, '_annotate_entities', side_effect=fail_second):
            response = self.post(
                    self.change(self.task_a, None, 1, 11),
                    self.change(self.task_b, None, 373, 405))
        self.assertEqual(2, len(calls))
        self.assertEqual(400, response.status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(set(), self.neurons_with(self.task_b))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_refused_link_removal_rolls_back_everything(self):
        self.link(self.task_a, SKELETON_373_NEURON)
        refused = ClassInstanceClassInstance.objects.get(
                class_instance_b__name=self.task_a)
        with patch.object(annotation, '_remove_annotation',
                return_value=([], [refused], False, 1)):
            response = self.post(
                    self.change(self.task_b, None, 373, 405),
                    self.change(self.task_a, 373, 1, 11))
        self.assertEqual(409, response.status_code)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(set(), self.neurons_with(self.task_b))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_already_root_reports_not_rerooted(self):
        results = self.results(self.post(self.change(self.task_a, None, 1, 7)))
        self.assertEqual([{'annotation': self.task_a, 'status': 'applied',
                'reason': None, 'rerooted': False}], results)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_validation_sees_state_under_locks(self):
        # Another task of the same study is linked to the target after the
        # request arrived, but before the neuron locks are taken.
        lock_neurons = task_links._lock_neurons

        def link_concurrently(*args, **kwargs):
            if not self.neurons_with(self.task_b):
                self.link(self.task_b, SKELETON_1_NEURON)
            return lock_neurons(*args, **kwargs)

        with patch.object(task_links, '_lock_neurons', side_effect=link_concurrently):
            results = self.results(self.post(self.change(self.task_a, None, 1, 11)))
        self.assertEqual('skipped', results[0]['status'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_treenodes_are_locked_before_neurons(self):
        events = []
        lock_treenodes = task_links._lock_treenodes
        lock_neurons = task_links._lock_neurons

        def record(name, lock):
            def locked(project_id, ids):
                events.append((name, sorted(set(ids))))
                return lock(project_id, ids)
            return locked

        with patch.object(task_links, '_lock_treenodes', record('treenodes', lock_treenodes)), \
                patch.object(task_links, '_lock_neurons', record('neurons', lock_neurons)):
            self.results(self.post(
                    self.change(self.task_a, None, 373, 405),
                    self.change(self.task_b, None, 1, 11)))
        self.assertEqual([('treenodes', [11, 405]),
                ('neurons', [SKELETON_1_NEURON, SKELETON_373_NEURON])], events)

    def test_duplicate_annotations_rejected(self):
        response = self.post(
                self.change(self.task_a, None, 1, 11),
                self.change(self.task_a, None, 373, 405))
        self.assertEqual(400, response.status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))

    def test_two_changes_targeting_one_skeleton_rejected(self):
        response = self.post(
                self.change(self.task_a, None, 1, 11),
                self.change(self.task_b, None, 1, 13))
        self.assertEqual(400, response.status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(set(), self.neurons_with(self.task_b))

    def test_malformed_body_rejected(self):
        response = self.client.post(self.url, json.dumps({'changes': [{
                'annotation': self.task_a, 'skeleton_id': 'one'}]}),
                content_type='application/json')
        self.assertEqual(400, response.status_code)



class DeadlineTaskLinksApiTests(TaskLinksFixtures, CatmaidApiTestCase):
    def test_apply_before_deadline_reports_applied_and_skipped_changes(self):
        apply = self.operation(operation_changes=[
            self.change(self.task_a, None, 1, 11),
            self.change(self.task_b, 1, 373, 405),
        ])
        response = self.post_operation(apply)
        self.assertStatus(response)
        completed = response.json()
        self.assertEqual('completed', completed['state'])
        self.assertEqual({'annotation': self.task_a, 'status': 'applied',
                'reason': None, 'rerooted': True}, completed['results'][0])
        self.assertEqual('skipped', completed['results'][1]['status'])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_recovery_before_deadline_is_pending_without_mutations(self):
        apply = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        response = self.post_operation(self.recovery(apply))
        self.assertStatus(response)
        self.assertEqual({'state': 'pending', 'results': []}, response.json())
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

        apply_response = self.post_operation(apply)
        self.assertStatus(apply_response)
        self.assertEqual('completed', apply_response.json()['state'])

    def test_expired_apply_never_changes_annotation_or_root(self):
        apply = self.operation(deadline=1,
                operation_changes=[self.change(self.task_a, None, 1, 11)])
        response = self.post_operation(apply)
        self.assertStatus(response)
        self.assertEqual({'state': 'expired', 'results': []}, response.json())
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_recovery_after_deadline_settles_without_changing_existing_links_or_roots(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        self.link(self.task_b, SKELETON_373_NEURON)

        response = self.post_operation(self.operation(deadline=1, recover=True))
        self.assertStatus(response)
        self.assertEqual({'state': 'settled', 'results': []}, response.json())
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_b))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_rejects_malformed_operation_envelopes_and_cross_study_annotations(self):
        apply = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        recovery = self.recovery(apply)
        for payload in (
                dict(apply, operation_changes=[self.change(self.task_b.replace(
                        str(self.study_id), str(uuid.uuid4())), None, 1, 11)]),
                dict(apply, study_id='AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'),
                dict(apply, operation_id=apply['operation_id'].replace('-', '')),
                dict(apply, cancel=True),
                dict(recovery, recover=False),
                dict(recovery, recover=1),
                dict(apply, recover=True),
                dict(recovery, operation_changes=[]),
                dict(apply, operation_changes=[]),
                dict(apply, changes=apply['operation_changes']),
                dict(apply, operation_changes=[dict(apply['operation_changes'][0], unrelated=True)]),
                *({key: value for key, value in apply.items() if key != missing}
                  for missing in ('study_id', 'operation_id', 'deadline', 'operation_changes')),
                *(dict(apply, deadline=value) for value in
                  (None, True, 0, -1, 1.5, '1', 253402300800))):
            with self.subTest(payload=payload):
                self.assertEqual(400, self.post_operation(payload).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_failed_apply_rolls_back_mutations(self):
        apply = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        original = task_links._apply

        def apply_then_fail(*args, **kwargs):
            original(*args, **kwargs)
            raise ValueError('Simulated failure after annotations and reroot')

        with patch.object(task_links, '_apply', side_effect=apply_then_fail):
            response = self.post_operation(apply)
        self.assertEqual(400, response.status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        retry = self.post_operation(apply)
        self.assertStatus(retry)
        self.assertEqual('applied', retry.json()['results'][0]['status'])


class DeadlineTaskLinksConcurrencyTests(TaskLinksFixtures, CatmaidApiTransactionTestCase):
    def overlapping_request(self, pool, call, *, require_lock=False):
        backend_ids = Queue()

        def request():
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute("SELECT set_config('lock_timeout', '10s', false), "
                            "pg_backend_pid()")
                    backend_ids.put(cursor.fetchone()[1])
                return call()
            finally:
                connections['default'].close()

        waiting = pool.submit(request)
        backend_id = backend_ids.get(timeout=10)
        deadline = monotonic() + 5
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            apply_backend_id = cursor.fetchone()[0]
            while not waiting.done():
                cursor.execute('SELECT pg_blocking_pids(%s)', [backend_id])
                if apply_backend_id in cursor.fetchone()[0]:
                    return waiting
                self.assertLess(monotonic(), deadline, 'Concurrent request neither waited nor completed')
                sleep(0.01)
        self.assertFalse(require_lock, 'Concurrent request completed before the study lock was released')
        return waiting

    def test_independent_applies_cannot_link_one_task_to_different_skeletons(self):
        # An existing annotation without a holder avoids serialization during
        # creation of the annotation object itself.
        annotation_class = Class.objects.get(project_id=self.test_project_id,
                class_name='annotation')
        ClassInstance.objects.create(project_id=self.test_project_id,
                class_column=annotation_class, user_id=self.test_user_id, name=self.task_a)
        first = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        second = self.operation(operation_changes=[self.change(self.task_a, None, 373, 405)])
        second_client = TaskLinkTestClient()
        second_client.cookies.update(self.client.cookies)
        original = task_links._apply
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, plans):
                original(project_id, user, plans)
                if plans[0].skeleton_id == 1:
                    waiters.append(self.overlapping_request(pool,
                            lambda: self.post_operation(second, second_client)))

            with patch.object(task_links, '_apply', side_effect=apply_then_overlap):
                first_response = self.post_operation(first)
                self.assertStatus(first_response)
                second_response = waiters[0].result(timeout=10)
        self.assertStatus(second_response)
        self.assertEqual('applied', first_response.json()['results'][0]['status'])
        self.assertEqual('skipped', second_response.json()['results'][0]['status'])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(377, self.root(373))

    def test_native_reroot_reads_topology_after_concurrent_task_link_commits(self):
        apply = self.operation(operation_changes=[self.change(self.task_a, None, 373, 407)])
        native = Client()
        native.cookies.update(self.client.cookies)
        original = task_links._apply
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(*args):
                original(*args)
                waiters.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/reroot' % self.test_project_id, {'treenode_id': 405})))

            with patch.object(task_links, '_apply', side_effect=apply_then_overlap):
                apply_response = self.post_operation(apply)
                self.assertStatus(apply_response)
            reroot_response = waiters[0].result(timeout=10)
        self.assertStatus(reroot_response)
        parents = dict(Treenode.objects.filter(skeleton_id=373).values_list('id', 'parent_id'))
        self.assertEqual({405}, {node for node, parent in parents.items() if parent is None})
        self.assertEqual(405, parents[407])
        self.assertEqual(405, parents[377])

    def database_deadline(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT floor(extract(epoch FROM clock_timestamp()))::bigint + 3')
            return cursor.fetchone()[0]

    def wait_until_database_deadline(self, deadline):
        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT pg_sleep(GREATEST(0, extract(epoch FROM
                    (to_timestamp(%s) - clock_timestamp()))))
            ''', [deadline])

    def test_apply_queued_before_deadline_expires_if_admitted_after_cutoff(self):
        first = self.operation(operation_changes=[self.change(self.task_a, None, 1, 11)])
        queued = self.operation(deadline=self.database_deadline(),
                operation_changes=[self.change(self.task_b, None, 373, 405)])
        second_client = TaskLinkTestClient()
        second_client.cookies.update(self.client.cookies)
        original = task_links._apply
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_and_hold_study(project_id, user, plans):
                original(project_id, user, plans)
                if plans[0].annotation == self.task_a:
                    waiters.append(self.overlapping_request(pool,
                            lambda: self.post_operation(queued, second_client), require_lock=True))
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT clock_timestamp() < to_timestamp(%s)',
                                [queued['deadline']])
                        self.assertTrue(cursor.fetchone()[0], 'Apply was not queued before cutoff')
                    self.wait_until_database_deadline(queued['deadline'])

            with patch.object(task_links, '_apply', side_effect=apply_and_hold_study):
                first_response = self.post_operation(first)
                self.assertStatus(first_response)
                second_response = waiters[0].result(timeout=10)

        self.assertStatus(second_response)
        self.assertEqual({'state': 'expired', 'results': []}, second_response.json())
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(set(), self.neurons_with(self.task_b))
        self.assertEqual(377, self.root(373))

    def overlapping_apply_and_recovery(self, *, rollback=False):
        apply = self.operation(deadline=self.database_deadline(),
                operation_changes=[self.change(self.task_a, None, 1, 11)])
        recovery = self.recovery(apply)
        recovery_client = TaskLinkTestClient()
        recovery_client.cookies.update(self.client.cookies)
        original = task_links._apply
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_and_wait_for_recovery(*args, **kwargs):
                original(*args, **kwargs)
                self.wait_until_database_deadline(apply['deadline'])
                waiters.append(self.overlapping_request(pool,
                        lambda: self.post_operation(recovery, recovery_client), require_lock=True))
                if rollback:
                    raise ValueError('Simulated rollback of in-flight apply')

            with patch.object(task_links, '_apply', side_effect=apply_and_wait_for_recovery):
                apply_response = self.post_operation(apply)
                self.assertEqual(400 if rollback else 200, apply_response.status_code,
                        apply_response.content)
            self.assertTrue(waiters, 'Recovery did not overlap the admitted apply')
            recovery_response = waiters[0].result(timeout=10)
        self.assertStatus(recovery_response)
        return apply, apply_response, recovery_response.json()

    def test_recovery_waits_for_admitted_apply_to_commit(self):
        _, apply_response, recovered = self.overlapping_apply_and_recovery()
        self.assertEqual('completed', apply_response.json()['state'])
        self.assertEqual({'state': 'settled', 'results': []}, recovered)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_recovery_waits_for_admitted_apply_to_roll_back(self):
        apply, _, recovered = self.overlapping_apply_and_recovery(rollback=True)
        self.assertEqual({'state': 'settled', 'results': []}, recovered)
        late_apply = self.post_operation(apply)
        self.assertStatus(late_apply)
        self.assertEqual({'state': 'expired', 'results': []}, late_apply.json())
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
