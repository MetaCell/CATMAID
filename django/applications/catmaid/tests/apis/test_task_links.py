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

from catmaid.control import annotation, skeleton, task_links
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
        self.url = '/%d/skeletons/task-link' % self.test_project_id
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

    def change(self, name, skeleton_id, reroot):
        return {
            'annotation': name,
            'skeleton_id': skeleton_id,
            'reroot_treenode_id': reroot,
        }

    def post(self, change, client=None):
        return (client or self.client).post(self.url, change, content_type='application/json')

    def result(self, response):
        self.assertStatus(response)
        return response.json()


class TaskLinksApiTests(TaskLinksFixtures, CatmaidApiTestCase):
    def test_native_annotator_cannot_link_task(self):
        native = Client()
        native.cookies.update(self.client.cookies)
        self.assertEqual(403, self.post(self.change(self.task_a, 1, 11), client=native).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_assignment_adds_link_and_reroots(self):
        result = self.result(self.post(self.change(self.task_a, 1, 11)))
        self.assertEqual({'skeleton_id': 1, 'rerooted': True}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_already_root_only_links(self):
        result = self.result(self.post(self.change(self.task_a, 1, 7)))
        self.assertEqual({'skeleton_id': 1, 'rerooted': False}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_assigned_task_cannot_move_or_reroot(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        for skeleton_id, node in ((1, 11), (373, 405)):
            with self.subTest(skeleton_id=skeleton_id):
                self.assertEqual(409, self.post(self.change(self.task_a, skeleton_id, node)).status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_target_held_by_same_study_is_rejected(self):
        self.link(self.task_b, SKELETON_1_NEURON)
        self.assertEqual(409, self.post(self.change(self.task_a, 1, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_target_held_by_other_study_is_available(self):
        self.link(task_annotation(uuid.uuid4()), SKELETON_1_NEURON)
        self.assertEqual(1, self.result(self.post(self.change(self.task_a, 1, 11)))['skeleton_id'])

    def test_target_neuron_requires_exactly_one_skeleton(self):
        ClassInstanceClassInstance.objects.filter(
                project_id=self.test_project_id, relation__relation_name='model_of',
                class_instance_a_id=373).update(class_instance_b_id=SKELETON_1_NEURON)
        self.assertEqual(409, self.post(self.change(self.task_a, 1, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))

    def test_reroot_treenode_must_belong_to_target(self):
        self.assertEqual(409, self.post(self.change(self.task_a, 1, 405)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_reroot_error_rolls_back_annotation(self):
        original = skeleton._reroot_skeleton

        def fail_after_reroot(*args):
            original(*args)
            raise ValueError('Simulated failure after changing the root')

        with patch.object(skeleton, '_reroot_skeleton', side_effect=fail_after_reroot):
            self.assertEqual(400, self.post(self.change(self.task_a, 1, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_malformed_requests_leave_task_unlinked(self):
        valid = self.change(self.task_a, 1, 11)
        for payload in ({'changes': [valid]}, dict(valid, expected_skeleton_id=None),
                dict(valid, annotation='ordinary'), dict(valid, skeleton_id=2**63),
                dict(valid, reroot_treenode_id=True)):
            with self.subTest(payload=payload):
                self.assertEqual(400, self.post(payload).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))


class TaskLinksConcurrencyTests(TaskLinksFixtures, CatmaidApiTransactionTestCase):
    def overlapping_request(self, pool, call):
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
        return waiting

    def test_independent_applies_cannot_link_one_task_to_different_skeletons(self):
        # An existing annotation without a holder avoids serialization during
        # creation of the annotation object itself.
        annotation_class = Class.objects.get(project_id=self.test_project_id,
                class_name='annotation')
        ClassInstance.objects.create(project_id=self.test_project_id,
                class_column=annotation_class, user_id=self.test_user_id, name=self.task_a)
        first = self.change(self.task_a, 1, 11)
        second = self.change(self.task_a, 373, 405)
        second_client = TaskLinkTestClient()
        second_client.cookies.update(self.client.cookies)
        original = task_links._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, change):
                result = original(project_id, user, change)
                if change.skeleton_id == 1:
                    waiters.append(self.overlapping_request(pool,
                            lambda: self.post(second, client=second_client)))
                return result

            with patch.object(task_links, '_assign', side_effect=apply_then_overlap):
                first_response = self.post(first)
                self.assertStatus(first_response)
                second_response = waiters[0].result(timeout=10)
        self.assertEqual(1, self.result(first_response)['skeleton_id'])
        self.assertEqual(409, second_response.status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(377, self.root(373))

    def test_concurrent_tasks_in_same_study_cannot_claim_one_skeleton(self):
        first = self.change(self.task_a, 1, 11)
        second = self.change(self.task_b, 1, 13)
        client = TaskLinkTestClient()
        client.cookies.update(self.client.cookies)
        original = task_links._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, change):
                result = original(project_id, user, change)
                if change.annotation == self.task_a:
                    waiters.append(self.overlapping_request(pool, lambda: self.post(second, client=client)))
                return result

            with patch.object(task_links, '_assign', side_effect=apply_then_overlap):
                self.assertStatus(self.post(first))
                response = waiters[0].result(timeout=10)

        self.assertEqual(409, response.status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(set(), self.neurons_with(self.task_b))
        self.assertEqual(11, self.root(1))

    def test_native_reroot_reads_topology_after_concurrent_task_link_commits(self):
        change = self.change(self.task_a, 373, 407)
        native = Client()
        native.cookies.update(self.client.cookies)
        original = task_links._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(*args):
                result = original(*args)
                waiters.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/reroot' % self.test_project_id, {'treenode_id': 405})))
                return result

            with patch.object(task_links, '_assign', side_effect=apply_then_overlap):
                apply_response = self.post(change)
                self.assertStatus(apply_response)
            reroot_response = waiters[0].result(timeout=10)
        self.assertStatus(reroot_response)
        parents = dict(Treenode.objects.filter(skeleton_id=373).values_list('id', 'parent_id'))
        self.assertEqual({405}, {node for node, parent in parents.items() if parent is None})
        self.assertEqual(405, parents[407])
        self.assertEqual(405, parents[377])
