# -*- coding: utf-8 -*-

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from time import monotonic, sleep, time
from unittest.mock import patch

from django.db import connection, connections
from django.test import Client
from django.test.client import ClientHandler

from catmaid.control import annotation, skeleton, task_links
from catmaid.models import (Class, ClassInstance, ClassInstanceClassInstance,
        Sampler, SamplerDomain, SamplerDomainType, SamplerState, Treenode, User)
from catmaid.source_fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX
from catmaid.state import make_nocheck_state

from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


SKELETON_1_NEURON = 2
SKELETON_373_NEURON = 374


def task_annotation(study_id):
    return 'neuroglass:study:%s:task:%s' % (study_id, uuid.uuid4())


class TrustedTaskLinkHandler(ClientHandler):
    cutoff_ms = None

    def get_response(self, request):
        # Stand in for the installed overlay's server-side grant.
        request._neuroglass_task_link_authorized = True
        request._neuroglass_task_link_cutoff_ms = (
                self.cutoff_ms if self.cutoff_ms is not None else int(time() * 1000) + 60_000)
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

    def change(self, name, node_id, radius=1):
        node = Treenode.objects.get(pk=node_id)
        return {
            'annotation': name,
            'coordinate': [node.location_x, node.location_y, node.location_z],
            'radius': radius,
        }

    def move_node_near(self, node_id, reference_id, dx=0):
        x, y, z = self.change(self.task_a, reference_id)['coordinate']
        Treenode.objects.filter(pk=node_id).update(location_x=x + dx, location_y=y, location_z=z)

    def post(self, change, client=None):
        return (client or self.client).post(self.url, change, content_type='application/json')

    def result(self, response):
        self.assertStatus(response)
        return response.json()


class TaskLinksApiTests(TaskLinksFixtures, CatmaidApiTestCase):
    def test_native_annotator_cannot_link_task(self):
        native = Client()
        native.cookies.update(self.client.cookies)
        self.assertEqual(403, self.post(self.change(self.task_a, 11), client=native).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_expired_start_cannot_link_or_reroot(self):
        self.client.handler.cutoff_ms = 1

        response = self.post(self.change(self.task_a, 11))

        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_assignment_adds_link_and_reroots(self):
        self.move_node_near(405, 11, dx=40)
        result = self.result(self.post(self.change(self.task_a, 11, radius=100)))
        self.assertEqual({'skeleton_id': 1, 'rerooted': True}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(11, self.root(1))

    def test_already_root_only_links(self):
        result = self.result(self.post(self.change(self.task_a, 7)))
        self.assertEqual({'skeleton_id': 1, 'rerooted': False}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_fast_split_keeps_task_link_on_the_original_skeleton(self):
        self.assertEqual(1, self.result(self.post(self.change(self.task_a, 7)))['skeleton_id'])
        provenance = SOURCE_FRAGMENT_ANNOTATION_PREFIX + 'imported-fragment'
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {name: {'user_id': self.test_user_id}
                    for name in ('ordinary annotation', provenance)}, allow_reserved=True)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id,
                {'treenode_id': 11}))

        self.assertEqual(1, result['existing_skeleton_id'])
        new_skeleton = result['new_skeleton_id']
        self.assertNotEqual(1, new_skeleton)
        self.assertEqual(7, self.root(1))
        self.assertEqual(11, self.root(new_skeleton))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        for skeleton_id in (1, new_skeleton):
            self.assertLessEqual({'ordinary annotation', provenance},
                    annotation.annotations_for_skeleton(self.test_project_id, skeleton_id).keys())

    def test_split_moves_ordinary_annotations_off_original_when_its_map_is_empty(self):
        self.link('ordinary label', SKELETON_1_NEURON)
        label = ClassInstance.objects.get(project_id=self.test_project_id, name='ordinary label')
        self.link('meta label', label.id)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': json.dumps({'ordinary label': self.test_user_id}),
        }))

        self.assertEqual({}, annotation.annotations_for_skeleton(self.test_project_id, 1))
        self.assertEqual({'ordinary label': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))
        self.assertTrue(ClassInstance.objects.filter(pk=label.id).exists())
        self.assertEqual({label.id}, self.neurons_with('meta label'))

    def test_split_refuses_a_preview_whose_parent_changed_during_start(self):
        self.assertEqual(7, Treenode.objects.get(pk=11).parent_id)
        self.assertEqual(11, Treenode.objects.get(pk=13).parent_id)
        self.assertStatus(self.post(self.change(self.task_a, 13)))
        self.assertEqual(13, Treenode.objects.get(pk=11).parent_id)
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))

        split = self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'expected_parent_id': 7,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': '{}',
        })

        self.assertEqual(400, split.status_code, split.content)
        self.assertIn('The split edge changed', split.json()['error'])
        self.assertEqual(original_nodes, set(Treenode.objects.filter(skeleton_id=1)
                .values_list('id', flat=True)))
        self.assertEqual(13, self.root(1))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))

    def test_confirmed_split_does_not_duplicate_the_task_link(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        self.link('shared label', SKELETON_1_NEURON)
        selected = json.dumps({self.task_a: self.test_user_id, 'shared label': self.test_user_id})

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'expected_parent_id': 7,
            'upstream_annotation_map': selected,
            'downstream_annotation_map': selected,
        }))

        self.assertEqual(1, result['existing_skeleton_id'])
        self.assertEqual(7, self.root(1))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual({'shared label': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))

    def test_confirmed_split_keeps_task_link_when_all_ordinary_labels_go_to_new_skeleton(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        self.link('selected label', SKELETON_1_NEURON)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': json.dumps({
                self.task_a: self.test_user_id,
                self.task_b: self.test_user_id,
                'selected label': self.test_user_id,
            }),
        }))

        self.assertEqual({self.task_a: self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, 1))
        self.assertEqual({'selected label': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))
        self.assertEqual(set(), self.neurons_with(self.task_b))

    def test_confirmed_split_keeps_task_link_omitted_from_both_maps(self):
        self.link(self.task_a, SKELETON_1_NEURON)

        self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': '{}',
        }))

        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))

    def test_assigned_task_cannot_move_or_reroot(self):
        self.link(self.task_a, SKELETON_1_NEURON)
        for skeleton_id, node in ((1, 11), (373, 405)):
            with self.subTest(skeleton_id=skeleton_id):
                self.assertEqual(409, self.post(self.change(self.task_a, node)).status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_chooses_next_skeleton_if_closest_is_held_by_same_study(self):
        self.move_node_near(405, 11, dx=40)
        self.link(self.task_b, SKELETON_1_NEURON)

        result = self.result(self.post(self.change(self.task_a, 11, radius=100)))

        self.assertEqual({'skeleton_id': 373, 'rerooted': True}, result)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_b))
        self.assertEqual(405, self.root(373))
        self.assertEqual(7, self.root(1))

    def test_chooses_next_skeleton_if_closest_is_locked_by_another_user(self):
        self.move_node_near(405, 11, dx=40)
        lock_owner = User.objects.get(username='test0')
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {'locked': {'user_id': lock_owner.id}})

        result = self.result(self.post(self.change(self.task_a, 11, radius=100)))

        self.assertEqual({'skeleton_id': 373, 'rerooted': True}, result)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(405, self.root(373))

    def test_can_assign_a_skeleton_locked_by_a_user_in_the_edit_domain(self):
        lock_owner = User.objects.get(username='test1')
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {'locked': {'user_id': lock_owner.id}})

        result = self.result(self.post(self.change(self.task_a, 11)))

        self.assertEqual({'skeleton_id': 1, 'rerooted': True}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))

    def test_skips_a_candidate_whose_new_root_would_break_a_sampler(self):
        self.move_node_near(405, 13, dx=40)
        sampler = Sampler.objects.create(
                project_id=self.test_project_id, user_id=self.test_user_id, skeleton_id=1,
                interval_length=10, interval_error=0,
                sampler_state=SamplerState.objects.create(name='Test sampler'))
        # Moving the root to node 13 puts the domain start on the wrong side.
        SamplerDomain.objects.create(
                project_id=self.test_project_id, user_id=self.test_user_id,
                sampler=sampler, start_node_id=11,
                domain_type=SamplerDomainType.objects.create(name='Test domain'))

        result = self.result(self.post(self.change(self.task_a, 13, radius=100)))

        self.assertEqual({'skeleton_id': 373, 'rerooted': True}, result)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(405, self.root(373))

    def test_breaks_distance_ties_by_skeleton_then_node_id(self):
        self.move_node_near(405, 11)
        self.move_node_near(13, 11)

        result = self.result(self.post(self.change(self.task_a, 11)))

        self.assertEqual(1, result['skeleton_id'])
        self.assertEqual(11, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_reports_no_match_when_radius_excludes_free_candidates(self):
        self.move_node_near(405, 11, dx=40)
        self.link(self.task_b, SKELETON_1_NEURON)

        response = self.post(self.change(self.task_a, 11, radius=39))

        self.assertEqual(409, response.status_code)
        self.assertEqual({'code': 'task_link_no_match'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_target_held_by_other_study_is_available(self):
        self.link(task_annotation(uuid.uuid4()), SKELETON_1_NEURON)
        self.assertEqual(1, self.result(self.post(self.change(self.task_a, 11)))['skeleton_id'])

    def test_target_neuron_requires_exactly_one_skeleton(self):
        ClassInstanceClassInstance.objects.filter(
                project_id=self.test_project_id, relation__relation_name='model_of',
                class_instance_a_id=373).update(class_instance_b_id=SKELETON_1_NEURON)
        self.assertEqual(409, self.post(self.change(self.task_a, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))

    def test_search_result_must_still_belong_to_the_target_at_write(self):
        with patch.object(task_links, '_nearest_candidate', return_value=(1, 405)):
            self.assertEqual(409, self.post(self.change(self.task_a, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_reroot_error_rolls_back_annotation(self):
        original = skeleton._reroot_skeleton

        def fail_after_reroot(*args):
            original(*args)
            raise ValueError('Simulated failure after changing the root')

        with patch.object(skeleton, '_reroot_skeleton', side_effect=fail_after_reroot):
            self.assertEqual(400, self.post(self.change(self.task_a, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_malformed_requests_leave_task_unlinked(self):
        valid = self.change(self.task_a, 11)
        old = {'annotation': self.task_a, 'skeleton_id': 1, 'reroot_treenode_id': 11}
        for payload in (old, {'changes': [valid]}, dict(valid, annotation='ordinary'),
                dict(valid, coordinate=[[1, 2, 3], [4, 5, 6]]),
                dict(valid, coordinate=[1, 2]), dict(valid, coordinate=[1, 'two', 3]),
                dict(valid, coordinate=[True, 0, 0]), dict(valid, coordinate=[10**400, 0, 0]),
                dict(valid, radius=0), dict(valid, radius=-1), dict(valid, radius=1e308)):
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
        first = self.change(self.task_a, 11)
        second = self.change(self.task_a, 405)
        second_client = TaskLinkTestClient()
        second_client.cookies.update(self.client.cookies)
        original = task_links._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, change):
                result = original(project_id, user, change)
                if result[0] == 1:
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
        first = self.change(self.task_a, 11)
        second = self.change(self.task_b, 13)
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
        change = self.change(self.task_a, 407)
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


    def test_split_rechecks_parent_after_task_link_reroots_same_node(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        native = Client()
        native.cookies.update(self.client.cookies)
        original = task_links._assign
        splits = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def assign_then_split(*args):
                result = original(*args)
                # Start has rerooted but hasn't committed. Split can still see
                # node 11's old parent until it acquires the topology lock.
                splits.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/split' % self.test_project_id, {'treenode_id': 11})))
                return result

            with patch.object(task_links, '_assign', side_effect=assign_then_split):
                self.assertStatus(self.post(self.change(self.task_a, 11)))
            split = splits[0].result(timeout=10)

        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a),
                'Splitting at the new root must not duplicate the task annotation')
        self.assertEqual(original_nodes,
                set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True)),
                'The linked skeleton must retain its nodes')
        self.assertEqual(11, self.root(1))
        self.assertIn(split.status_code, (400, 409), split.content)

    def test_task_link_refuses_candidate_moved_outside_the_radius_before_lock(self):
        payload = self.change(self.task_a, 11)
        nearest_candidate = task_links._nearest_candidate

        def move_node(node_id):
            try:
                return Treenode.objects.filter(pk=node_id).update(
                        location_x=payload['coordinate'][0] + 100)
            finally:
                connections['default'].close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            def choose_then_move(*args):
                candidate = nearest_candidate(*args)
                self.assertEqual((1, 11), candidate)
                self.assertEqual(1, pool.submit(move_node, 11).result(timeout=5))
                return candidate

            with patch.object(task_links, '_nearest_candidate', side_effect=choose_then_move):
                response = self.post(payload)

        self.assertEqual(409, response.status_code, response.content)
        self.assertIn('moved during assignment', response.json()['error'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertEqual(7, self.root(1))

    def test_task_link_refuses_skeleton_deleted_before_neuron_lock(self):
        native = Client()
        native.force_login(User.objects.get(username='admin'))
        target_neuron = task_links._target_neuron

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = []

            def delete_before_neuron_lock(*args):
                if not pending:
                    pending.append(self.overlapping_request(pool, lambda: native.post(
                            '/%d/skeletons/1/delete' % self.test_project_id)))
                return target_neuron(*args)

            with patch.object(task_links, '_target_neuron', side_effect=delete_before_neuron_lock):
                response = self.post(self.change(self.task_a, 13))
            deleted = pending[0].result(timeout=10)

        self.assertStatus(deleted)
        self.assertEqual(409, response.status_code, response.content)
        self.assertIn('must model exactly one neuron', response.json()['error'])
        self.assertEqual(set(), self.neurons_with(self.task_a))
        self.assertFalse(Treenode.objects.filter(skeleton_id=1).exists())

    def test_node_deletion_waits_for_task_link_reroot(self):
        self.assertEqual(11, Treenode.objects.get(pk=13).parent_id)
        self.assertEqual(7, Treenode.objects.get(pk=11).parent_id)
        native = Client()
        native.cookies.update(self.client.cookies)
        reroot = skeleton._reroot_skeleton

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = []

            def delete_while_reroot_holds_the_child(*args):
                pending.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/treenode/delete' % self.test_project_id, {
                            'treenode_id': 11, 'state': make_nocheck_state(),
                        })))
                return reroot(*args)

            with patch.object(skeleton, '_reroot_skeleton',
                    side_effect=delete_while_reroot_holds_the_child):
                response = self.post(self.change(self.task_a, 13))
            deleted = pending[0].result(timeout=10)

        self.assertEqual({'skeleton_id': 1, 'rerooted': True}, self.result(response))
        self.assertStatus(deleted)
        self.assertFalse(Treenode.objects.filter(pk=11).exists())
        self.assertEqual(13, self.root(1))
        self.assertEqual(13, Treenode.objects.get(pk=7).parent_id)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.task_a))

    def test_task_link_reroot_preserves_concurrent_insertion_on_ancestor_edge(self):
        native = Client()
        native.cookies.update(self.client.cookies)
        insertions = []
        parent = Treenode.objects.get(pk=7)
        child = Treenode.objects.get(pk=11)
        self.assertEqual(parent.id, child.parent_id)
        payload = {
            'parent_id': parent.id,
            'child_id': child.id,
            'x': (parent.location_x + child.location_x) / 2,
            'y': (parent.location_y + child.location_y) / 2,
            'z': (parent.location_z + child.location_z) / 2,
            'state': make_nocheck_state(),
        }
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))

        with ThreadPoolExecutor(max_workers=1) as pool:
            def insert_before_reroot_update(execute, sql, params, many, context):
                if 'SET parent_id = v.parent_id' in sql:
                    # Reroot has read the old path and is about to reverse it.
                    insertions.append(self.overlapping_request(pool, lambda: native.post(
                            '/%d/treenode/insert' % self.test_project_id, payload)))
                return execute(sql, params, many, context)

            with connection.execute_wrapper(insert_before_reroot_update):
                self.assertStatus(self.post(self.change(self.task_a, 13)))
            self.assertEqual(1, len(insertions))
            insert = insertions[0].result(timeout=10)

        self.assertEqual(13, self.root(1))
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        if insert.status_code == 200:
            new_node = insert.json()['treenode_id']
            self.assertEqual(original_nodes | {new_node}, set(parents))
            self.assertEqual(new_node, parents[parent.id],
                    'Reroot must preserve the successful insertion on the reversed edge')
            self.assertEqual(child.id, parents[new_node])
        else:
            self.assertIn(insert.status_code, (400, 409), insert.content)
            self.assertIn('child and parent', insert.json()['error'])
            self.assertEqual(original_nodes, set(parents))
            self.assertEqual(child.id, parents[parent.id])
