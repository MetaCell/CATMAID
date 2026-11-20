# -*- coding: utf-8 -*-

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import Queue
from threading import Event
from time import monotonic, sleep
from unittest.mock import patch

from django.db import connection, connections, transaction
from django.test import Client, override_settings

from catmaid.control import annotation, skeleton, annotation_matching
from catmaid.locks import SkeletonEditLockTimeout, lock_skeleton_topology
from catmaid.models import (Class, ClassInstance, ClassInstanceClassInstance,
        Sampler, SamplerDomain, SamplerDomainType, SamplerState, Treenode, User)
from catmaid.source_fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX
from catmaid.state import make_nocheck_state

from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


SKELETON_1_NEURON = 2
SKELETON_373_NEURON = 374


def annotation_name(group):
    return 'experiment:%s:target' % group


class AnnotationMatchingFixtures:
    def setUp(self):
        super().setUp()
        self.annotation_group = 'experiment-A'
        self.client = Client()
        self.url = '/%d/skeletons/annotate-nearest' % self.test_project_id
        self.annotation_a = annotation_name(self.annotation_group)
        self.annotation_b = 'experiment:experiment-A:other'
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
            'exclude_annotation_pattern': r'^experiment:experiment-A:',
        }

    def move_node_near(self, node_id, reference_id, dx=0):
        x, y, z = self.change(self.annotation_a, reference_id)['coordinate']
        Treenode.objects.filter(pk=node_id).update(location_x=x + dx, location_y=y, location_z=z)

    def post(self, change, client=None):
        return (client or self.client).post(self.url, change, content_type='application/json')

    def result(self, response):
        self.assertStatus(response)
        return response.json()


class AnnotationMatchingApiTests(AnnotationMatchingFixtures, CatmaidApiTestCase):
    def test_native_annotator_can_annotate_an_arbitrary_name(self):
        payload = self.change('My annotation', 11)
        payload.pop('exclude_annotation_pattern')
        result = self.result(self.post(payload))
        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with('My annotation'))
        self.assertEqual(11, self.root(1))

    def test_annotation_name_length_limit(self):
        oversized_name = 'a' * 256
        response = self.post(self.change(oversized_name, 11))
        self.assertEqual(400, response.status_code, response.content)
        self.assertEqual(set(), self.neurons_with(oversized_name))
        self.assertEqual(7, self.root(1))

        max_length_name = 'a' * 255
        result = self.result(self.post(self.change(max_length_name, 11)))
        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(max_length_name))

    def test_annotating_nearest_requires_project_annotate_permission(self):
        from guardian.shortcuts import remove_perm
        remove_perm('can_annotate', self.test_user, self.test_project)
        response = self.post(self.change(self.annotation_a, 11))
        self.assertEqual(403, response.status_code, response.content)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_existing_annotation_is_checked_before_search(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)
        with patch.object(annotation_matching, '_nearest_candidate',
                side_effect=AssertionError('An existing annotation must not trigger a search')):
            response = self.post(self.change(self.annotation_a, 405))
        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual(377, self.root(373))

    def test_annotation_exclusions_are_optional(self):
        self.link(self.annotation_b, SKELETON_1_NEURON)
        payload = self.change(self.annotation_a, 11)
        payload.pop('exclude_annotation_pattern')
        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, self.result(self.post(payload)))

    def test_invalid_annotation_pattern_leaves_no_annotation_or_reroot(self):
        payload = dict(self.change(self.annotation_a, 11), exclude_annotation_pattern='[')
        response = self.post(payload)
        self.assertEqual(400, response.status_code, response.content)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_annotation_match_requires_json(self):
        response = self.client.post(self.url, self.change(self.annotation_a, 11))
        self.assertEqual(415, response.status_code, response.content)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_assignment_adds_link_and_reroots(self):
        self.move_node_near(405, 11, dx=40)
        result = self.result(self.post(self.change(self.annotation_a, 11, radius=100)))
        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(11, self.root(1))

    def test_already_root_only_links(self):
        result = self.result(self.post(self.change(self.annotation_a, 7)))
        self.assertEqual({'skeleton_id': 1, 'root_id': 7}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))


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

    def test_assigned_annotation_cannot_move_or_reroot(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)
        for skeleton_id, node in ((1, 11), (373, 405)):
            with self.subTest(skeleton_id=skeleton_id):
                self.assertEqual(409, self.post(self.change(self.annotation_a, node)).status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_already_assigned_annotation_reports_conflict_when_no_other_skeleton_is_in_range(self):
        self.move_node_near(405, 11, dx=40)
        self.link(self.annotation_a, SKELETON_1_NEURON)

        response = self.post(self.change(self.annotation_a, 11, radius=39))

        self.assertEqual(409, response.status_code)
        self.assertEqual('AnnotationMatchConflict', response.json()['type'])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_chooses_next_skeleton_if_closest_is_excluded_by_pattern(self):
        self.move_node_near(405, 11, dx=40)
        self.link(self.annotation_b, SKELETON_1_NEURON)

        result = self.result(self.post(self.change(self.annotation_a, 11, radius=100)))

        self.assertEqual({'skeleton_id': 373, 'root_id': 405}, result)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_b))
        self.assertEqual(405, self.root(373))
        self.assertEqual(7, self.root(1))

    def test_chooses_next_skeleton_if_closest_is_locked_by_another_user(self):
        self.move_node_near(405, 11, dx=40)
        lock_owner = User.objects.get(username='test0')
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {'locked': {'user_id': lock_owner.id}})

        result = self.result(self.post(self.change(self.annotation_a, 11, radius=100)))

        self.assertEqual({'skeleton_id': 373, 'root_id': 405}, result)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(405, self.root(373))

    def test_can_assign_a_skeleton_locked_by_a_user_in_the_edit_domain(self):
        lock_owner = User.objects.get(username='test1')
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {'locked': {'user_id': lock_owner.id}})

        result = self.result(self.post(self.change(self.annotation_a, 11)))

        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, result)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    def test_native_sampler_rejection_rolls_back_without_trying_another_skeleton(self):
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

        response = self.post(self.change(self.annotation_a, 13, radius=100))

        self.assertEqual(400, response.status_code, response.content)
        self.assertIn('is used in a sampler', response.json()['error'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_breaks_distance_ties_by_skeleton_then_node_id(self):
        self.move_node_near(405, 11)
        self.move_node_near(13, 11)

        result = self.result(self.post(self.change(self.annotation_a, 11)))

        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, result)
        self.assertEqual(11, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_reports_no_match_when_radius_excludes_free_candidates(self):
        self.move_node_near(405, 11, dx=40)
        self.link(self.annotation_b, SKELETON_1_NEURON)

        response = self.post(self.change(self.annotation_a, 11, radius=39))

        self.assertEqual(409, response.status_code)
        self.assertEqual({'code': 'annotation_match_no_match'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))
        self.assertEqual(377, self.root(373))

    def test_target_with_nonmatching_annotation_is_available(self):
        self.link(annotation_name('experiment-B'), SKELETON_1_NEURON)
        self.assertEqual(1, self.result(self.post(self.change(self.annotation_a, 11)))['skeleton_id'])

    def test_target_neuron_requires_exactly_one_skeleton(self):
        ClassInstanceClassInstance.objects.filter(
                project_id=self.test_project_id, relation__relation_name='model_of',
                class_instance_a_id=373).update(class_instance_b_id=SKELETON_1_NEURON)
        self.assertEqual(409, self.post(self.change(self.annotation_a, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))

    def test_search_result_must_still_belong_to_the_target_at_write(self):
        for node_id, neuron_id, error in (
                (405, SKELETON_1_NEURON, 'reroot node is no longer on the target skeleton'),
                (11, SKELETON_373_NEURON, 'target skeleton changed during assignment')):
            with self.subTest(node_id=node_id, neuron_id=neuron_id):
                coordinate = self.change(self.annotation_a, node_id)['coordinate']
                candidate = (1, node_id, neuron_id, *coordinate)
                with patch.object(annotation_matching, '_nearest_candidate', return_value=candidate):
                    response = self.post(self.change(self.annotation_a, 11))
                self.assertEqual(409, response.status_code, response.content)
                self.assertIn(error, response.json()['error'])
                self.assertEqual(set(), self.neurons_with(self.annotation_a))
                self.assertEqual(7, self.root(1))

    def test_reroot_error_rolls_back_annotation(self):
        original = skeleton._reroot_skeleton_locked

        def fail_after_reroot(*args):
            original(*args)
            raise ValueError('Simulated failure after changing the root')

        with patch.object(skeleton, '_reroot_skeleton_locked', side_effect=fail_after_reroot):
            self.assertEqual(400, self.post(self.change(self.annotation_a, 11)).status_code)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_malformed_requests_leave_annotation_unassigned(self):
        valid = self.change(self.annotation_a, 11)
        old = {'annotation': self.annotation_a, 'skeleton_id': 1, 'reroot_treenode_id': 11}
        for payload in (old, {'changes': [valid]}, dict(valid, annotation=''), dict(valid, annotation='   '),
                dict(valid, annotation=23), dict(valid, annotation='label{n}'),
                dict(valid, exclude_annotation_pattern=23),
                dict(valid, coordinate=[[1, 2, 3], [4, 5, 6]]),
                dict(valid, coordinate=[1, 2]), dict(valid, coordinate=[1, 'two', 3]),
                dict(valid, coordinate=[True, 0, 0]), dict(valid, coordinate=[10**400, 0, 0]),
                dict(valid, radius=0), dict(valid, radius=-1), dict(valid, radius=1e308)):
            with self.subTest(payload=payload):
                self.assertEqual(400, self.post(payload).status_code)
        self.assertEqual(set(), self.neurons_with(self.annotation_a))


class AnnotationMatchingConcurrencyTests(AnnotationMatchingFixtures, CatmaidApiTransactionTestCase):

    @contextmanager
    def held_lock(self, sql, params):
        holder = connections['default'].copy(alias='matching_lock_holder')
        try:
            holder.set_autocommit(False)
            with holder.cursor() as cursor:
                cursor.execute(sql, params)
            yield
        finally:
            holder.rollback()
            holder.close()

    def held_topology_lock(self, skeleton_id):
        return self.held_lock('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))',
                ['catmaid-reroot:%s:%s' % (self.test_project_id, skeleton_id)])

    def assert_assignment_lock_timeout(self, held_lock):
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        change = self.change(self.annotation_a, 13)
        with held_lock, transaction.atomic(), connection.cursor() as cursor:
            # Fail promptly even if the assignment lock loses its timeout.
            cursor.execute("SET LOCAL lock_timeout = '400ms'")
            response = self.post(change)

        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual({'code': 'edit_lock_timeout'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(parents,
                dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id')))
        self.assertEqual(7, self.root(1))

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(self.post(change)))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(13, self.root(1))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_annotate_nearest_reports_annotation_lock_timeout_as_409(self):
        key = 'catmaid-annotation-match:%s:%s' % (self.test_project_id, self.annotation_a.lower())
        self.assert_assignment_lock_timeout(self.held_lock(
                'SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', [key]))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_annotate_nearest_reports_neuron_lock_timeout_as_409(self):
        self.assert_assignment_lock_timeout(self.held_lock(
                'SELECT id FROM class_instance WHERE id=%s FOR UPDATE', [SKELETON_1_NEURON]))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_annotate_nearest_reports_selected_node_lock_timeout_as_409(self):
        self.assert_assignment_lock_timeout(self.held_node_lock(13))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_annotate_nearest_reports_topology_lock_timeout_as_409(self):
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        change = self.change(self.annotation_a, 11)
        with self.held_topology_lock(1):
            response = self.post(change)
        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual({'code': 'edit_lock_timeout'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(parents,
                dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id')))
        self.assertEqual(7, self.root(1))

        self.assertEqual({'skeleton_id': 1, 'root_id': 11}, self.result(self.post(change)))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_native_reroot_reports_topology_lock_timeout_as_409(self):
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        url = '/%d/skeleton/reroot' % self.test_project_id
        with self.held_topology_lock(1):
            response = self.client.post(url, {'treenode_id': 11})
        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual({'code': 'edit_lock_timeout'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(parents,
                dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id')))
        self.assertEqual(7, self.root(1))

        self.assertStatus(self.client.post(url, {'treenode_id': 11}))
        self.assertEqual(11, self.root(1))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_topology_lock_timeout_restores_policy_and_releases_partial_locks(self):
        with self.held_topology_lock(373):
            for previous_timeout in (25, 2500):
                with self.subTest(previous_timeout=previous_timeout), transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT set_config('lock_timeout', %s, true)",
                                [f'{previous_timeout}ms'])
                        with self.assertRaises(ValueError) as failure:
                            lock_skeleton_topology(self.test_project_id, [373, 1, 1])
                        self.assertIsInstance(failure.exception, SkeletonEditLockTimeout)
                        cursor.execute("SELECT setting::integer FROM pg_settings WHERE name='lock_timeout'")
                        self.assertEqual(previous_timeout, cursor.fetchone()[0])
                    probe = connections['default'].copy(alias='reroot_lock_probe')
                    try:
                        with probe.cursor() as cursor:
                            cursor.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))',
                                    ['catmaid-reroot:%s:1' % self.test_project_id])
                            self.assertTrue(cursor.fetchone()[0], 'The partial advisory lock was not released')
                    finally:
                        probe.close()

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

    def test_independent_applies_cannot_link_one_annotation_to_different_skeletons(self):
        # An existing annotation without a holder avoids serialization during
        # creation of the annotation object itself.
        annotation_class = Class.objects.get(project_id=self.test_project_id,
                class_name='annotation')
        ClassInstance.objects.create(project_id=self.test_project_id,
                class_column=annotation_class, user_id=self.test_user_id, name=self.annotation_a)
        first = self.change(self.annotation_a, 11)
        second = self.change(self.annotation_a, 405)
        second_client = Client()
        second_client.cookies.update(self.client.cookies)
        original = annotation_matching._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, change):
                result = original(project_id, user, change)
                if result[0] == 1:
                    waiters.append(self.overlapping_request(pool,
                            lambda: self.post(second, client=second_client)))
                return result

            with patch.object(annotation_matching, '_assign', side_effect=apply_then_overlap):
                first_response = self.post(first)
                self.assertStatus(first_response)
                second_response = waiters[0].result(timeout=10)
        self.assertEqual(1, self.result(first_response)['skeleton_id'])
        self.assertEqual(409, second_response.status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(377, self.root(373))

    def test_concurrent_annotations_with_same_exclusion_cannot_claim_one_skeleton(self):
        first = self.change(self.annotation_a, 11)
        second = self.change(self.annotation_b, 13)
        client = Client()
        client.cookies.update(self.client.cookies)
        original = annotation_matching._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(project_id, user, change):
                result = original(project_id, user, change)
                if change.annotation == self.annotation_a:
                    waiters.append(self.overlapping_request(pool, lambda: self.post(second, client=client)))
                return result

            with patch.object(annotation_matching, '_assign', side_effect=apply_then_overlap):
                self.assertStatus(self.post(first))
                response = waiters[0].result(timeout=10)

        self.assertEqual(409, response.status_code)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual(set(), self.neurons_with(self.annotation_b))
        self.assertEqual(11, self.root(1))

    def test_native_reroot_reads_topology_after_concurrent_annotation_match_commits(self):
        change = self.change(self.annotation_a, 407)
        native = Client()
        native.cookies.update(self.client.cookies)
        original = annotation_matching._assign
        waiters = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def apply_then_overlap(*args):
                result = original(*args)
                waiters.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/reroot' % self.test_project_id, {'treenode_id': 405})))
                return result

            with patch.object(annotation_matching, '_assign', side_effect=apply_then_overlap):
                apply_response = self.post(change)
                self.assertStatus(apply_response)
            reroot_response = waiters[0].result(timeout=10)
        self.assertStatus(reroot_response)
        parents = dict(Treenode.objects.filter(skeleton_id=373).values_list('id', 'parent_id'))
        self.assertEqual({405}, {node for node, parent in parents.items() if parent is None})
        self.assertEqual(405, parents[407])
        self.assertEqual(405, parents[377])


    def test_split_rechecks_parent_after_annotation_match_reroots_same_node(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        native = Client()
        native.cookies.update(self.client.cookies)
        original = annotation_matching._assign
        splits = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def assign_then_split(*args):
                result = original(*args)
                # Start has rerooted but hasn't committed. Split can still see
                # node 11's old parent until it acquires the topology lock.
                splits.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/split' % self.test_project_id, {'treenode_id': 11})))
                return result

            with patch.object(annotation_matching, '_assign', side_effect=assign_then_split):
                self.assertStatus(self.post(self.change(self.annotation_a, 11)))
            split = splits[0].result(timeout=10)

        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a),
                'Splitting at the new root must not duplicate the assigned annotation')
        self.assertEqual(original_nodes,
                set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True)),
                'The linked skeleton must retain its nodes')
        self.assertEqual(11, self.root(1))
        self.assertIn(split.status_code, (400, 409), split.content)

    def test_annotation_match_refuses_candidate_moved_outside_the_radius_before_lock(self):
        self.assert_annotation_match_refuses_moved_candidate(radius=1)

    def test_annotation_match_refuses_candidate_moved_within_the_radius_before_lock(self):
        self.assert_annotation_match_refuses_moved_candidate(radius=101)

    def assert_annotation_match_refuses_moved_candidate(self, radius):
        payload = self.change(self.annotation_a, 11, radius=radius)
        nearest_candidate = annotation_matching._nearest_candidate

        def move_node(node_id):
            try:
                return Treenode.objects.filter(pk=node_id).update(
                        location_x=payload['coordinate'][0] + 100)
            finally:
                connections['default'].close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            def choose_then_move(*args):
                candidate = nearest_candidate(*args)
                self.assertEqual((1, 11), candidate[:2])
                self.assertEqual(1, pool.submit(move_node, 11).result(timeout=5))
                return candidate

            with patch.object(annotation_matching, '_nearest_candidate', side_effect=choose_then_move):
                response = self.post(payload)

        self.assertEqual(409, response.status_code, response.content)
        self.assertIn('moved during annotation', response.json()['error'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(7, self.root(1))

    def test_annotation_match_refuses_skeleton_deleted_before_neuron_lock(self):
        native = Client()
        native.force_login(User.objects.get(username='admin'))
        nearest_candidate = annotation_matching._nearest_candidate

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = []

            def choose_then_delete(*args):
                candidate = nearest_candidate(*args)
                pending.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeletons/1/delete' % self.test_project_id)))
                return candidate

            with patch.object(annotation_matching, '_nearest_candidate', side_effect=choose_then_delete):
                response = self.post(self.change(self.annotation_a, 13))
            deleted = pending[0].result(timeout=10)

        self.assertStatus(deleted)
        self.assertEqual(409, response.status_code, response.content)
        self.assertIn('target neuron no longer exists', response.json()['error'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertFalse(Treenode.objects.filter(skeleton_id=1).exists())

    def test_node_deletion_waits_for_annotation_match_reroot(self):
        self.assertEqual(11, Treenode.objects.get(pk=13).parent_id)
        self.assertEqual(7, Treenode.objects.get(pk=11).parent_id)
        native = Client()
        native.cookies.update(self.client.cookies)
        reroot = skeleton._reroot_skeleton_locked

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = []

            def delete_while_reroot_holds_the_child(*args):
                pending.append(self.overlapping_request(pool, lambda: native.post(
                        '/%d/treenode/delete' % self.test_project_id, {
                            'treenode_id': 11, 'state': make_nocheck_state(),
                        })))
                return reroot(*args)

            with patch.object(skeleton, '_reroot_skeleton_locked',
                    side_effect=delete_while_reroot_holds_the_child):
                response = self.post(self.change(self.annotation_a, 13))
            deleted = pending[0].result(timeout=10)

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(response))
        self.assertStatus(deleted)
        self.assertFalse(Treenode.objects.filter(pk=11).exists())
        self.assertEqual(13, self.root(1))
        self.assertEqual(13, Treenode.objects.get(pk=7).parent_id)
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    def edge_insertion(self, parent_id, child_id):
        parent = Treenode.objects.get(pk=parent_id)
        child = Treenode.objects.get(pk=child_id)
        return {
            'parent_id': parent_id, 'child_id': child_id, 'state': make_nocheck_state(),
            'x': (parent.location_x + child.location_x) / 2,
            'y': (parent.location_y + child.location_y) / 2,
            'z': (parent.location_z + child.location_z) / 2,
        }

    def held_node_lock(self, node_id):
        return self.held_lock('SELECT id FROM treenode WHERE id=%s FOR UPDATE', [node_id])

    def reparent_during_match(self, selected_id, moved_id, new_parent_id):
        moved = Treenode.objects.select_related('parent').get(pk=moved_id)
        request_state = json.dumps({
            'edition_time': moved.edition_time.isoformat(),
            'parent': [moved.parent_id, moved.parent.edition_time.isoformat()],
            'children': [[node.id, node.edition_time.isoformat()]
                         for node in Treenode.objects.filter(parent=moved)],
            'links': [],
        })
        return self.native_edit_during_match(selected_id,
                '/%d/treenodes/%d/parent' % (self.test_project_id, moved_id),
                {'parent_id': new_parent_id, 'state': request_state})

    def native_edit_during_match(self, selected_id, url, payload):
        native = Client()
        native.cookies.update(self.client.cookies)
        pending = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def start_edit_before_graph_lock(execute, sql, params, many, context):
                if 'ORDER BY t.id' in sql and 'FOR NO KEY UPDATE OF t' in sql and not pending:
                    pending.append(self.overlapping_request(pool, lambda: native.post(url, payload)))
                return execute(sql, params, many, context)

            with connection.execute_wrapper(start_edit_before_graph_lock):
                response = self.post(self.change(self.annotation_a, selected_id))
            self.assertEqual(1, len(pending), 'The test did not run the concurrent native edit')
            return response, pending[0].result(timeout=10)

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_reparent_to_selected_node_does_not_deadlock(self):
        # Node 407's parent is on a branch that rerooting at 403 leaves intact.
        response, reparented = self.reparent_during_match(403, 407, 403)
        self.assertEqual({'skeleton_id': 373, 'root_id': 403}, self.result(response))
        self.assertStatus(reparented)
        self.assertEqual(403, self.root(373))
        self.assertEqual(403, Treenode.objects.get(pk=407).parent_id)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_reparent_rejects_state_stale_after_matching_reroot(self):
        response, reparented = self.reparent_during_match(403, 403, 405)
        self.assertEqual({'skeleton_id': 373, 'root_id': 403}, self.result(response))
        self.assertEqual(400, reparented.status_code, reparented.content)
        self.assertEqual('StateMatchingError', reparented.json()['type'])

        # A caller that opts out of comparing state still cannot move the root.
        retry = self.client.post('/%d/treenodes/403/parent' % self.test_project_id, {
            'parent_id': 405, 'state': make_nocheck_state(),
        })
        self.assertEqual(400, retry.status_code, retry.content)
        self.assertIn('parent of the root node', retry.json()['error'])
        self.assertEqual(403, self.root(373))
        self.assertEqual(403, Treenode.objects.get(pk=377).parent_id)
        self.assertEqual(377, Treenode.objects.get(pk=405).parent_id)
        self.assertEqual({SKELETON_373_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_batch_move_waits_for_matching_before_checking_state(self):
        nodes = list(Treenode.objects.filter(pk__in=[7, 13]).order_by('id'))
        payload = {'state': json.dumps([
            [node.id, node.edition_time.isoformat()] for node in nodes
        ])}
        for index, node in enumerate(nodes):
            for field, value in enumerate((node.id, node.location_x + 1, node.location_y, node.location_z)):
                payload[f't[{index}][{field}]'] = value

        response, moved = self.native_edit_during_match(13,
                '/%d/node/update' % self.test_project_id, payload)

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(response))
        self.assertEqual(400, moved.status_code, moved.content)
        self.assertEqual('StateMatchingError', moved.json()['type'])
        self.assertEqual({node.id: node.location_x for node in nodes}, dict(
                Treenode.objects.filter(pk__in=[7, 13]).values_list('id', 'location_x')))
        self.assertEqual(13, self.root(1))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_insertion_on_selected_edge_does_not_deadlock(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        parent = Treenode.objects.get(pk=11)
        child = Treenode.objects.get(pk=13)
        insertion = self.edge_insertion(parent.id, child.id)
        insertion['state'] = json.dumps({
            'edition_time': parent.edition_time.isoformat(),
            'children': [[child.id, child.edition_time.isoformat()]],
        })
        native = Client()
        native.cookies.update(self.client.cookies)
        pending = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def insert_before_graph_lock(execute, sql, params, many, context):
                if 'ORDER BY t.id' in sql and 'FOR NO KEY UPDATE OF t' in sql and not pending:
                    pending.append(self.overlapping_request(pool, lambda: native.post(
                            '/%d/treenode/insert' % self.test_project_id, insertion)))
                    self.assertFalse(pending[0].done(), 'Native insertion did not wait')
                return execute(sql, params, many, context)

            with connection.execute_wrapper(insert_before_graph_lock):
                response = self.post(self.change(self.annotation_a, child.id))
            self.assertEqual(1, len(pending), 'The test did not run the native insertion')
            inserted = pending[0].result(timeout=10)

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(response))
        self.assertEqual(400, inserted.status_code, inserted.content)
        self.assertIn('state differs', inserted.json()['error'])
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        self.assertEqual(original_nodes, parents.keys())
        self.assertIsNone(parents[13])
        self.assertEqual(13, parents[11])
        self.assertEqual(11, parents[7])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_edge_insertion_waits_for_annotation_matching_reroot(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        insertion = self.edge_insertion(7, 11)
        native = Client()
        native.cookies.update(self.client.cookies)
        pending = []
        blocked = []

        with ThreadPoolExecutor(max_workers=1) as pool:
            def insert_before_parent_rewrite(execute, sql, params, many, context):
                if 'UPDATE treenode' in sql and 'v.parent_id' in sql and not pending:
                    waiting = self.overlapping_request(pool, lambda: native.post(
                            '/%d/treenode/insert' % self.test_project_id, insertion))
                    pending.append(waiting)
                    blocked.append(not waiting.done())
                return execute(sql, params, many, context)

            with connection.execute_wrapper(insert_before_parent_rewrite):
                response = self.post(self.change(self.annotation_a, 13))
            self.assertStatus(response)
            self.assertEqual(1, len(pending), 'The test did not overlap the native insertion')
            inserted = pending[0].result(timeout=10)

        self.assertTrue(blocked[0], 'Native insertion bypassed the reroot snapshot lock')
        self.assertEqual(400, inserted.status_code, inserted.content)
        self.assertIn('child and parent', inserted.json()['error'])
        self.assertEqual(original_nodes,
                set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True)))
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        self.assertIsNone(parents[13])
        self.assertEqual(13, parents[11])
        self.assertEqual(11, parents[7])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_annotation_matching_reroot_waits_for_edge_insertion(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        insertion = self.edge_insertion(7, 11)
        change = self.change(self.annotation_a, 13)
        matching = Client()
        matching.cookies.update(self.client.cookies)

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                inserted = self.result(self.client.post(
                        '/%d/treenode/insert' % self.test_project_id, insertion))['treenode_id']
                waiting = self.overlapping_request(pool, lambda: self.post(change, client=matching))
                self.assertFalse(waiting.done(), 'Reroot did not wait for native insertion')
            response = waiting.result(timeout=10)

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(response))
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        self.assertEqual(original_nodes | {inserted}, set(parents))
        self.assertIsNone(parents[13])
        self.assertEqual(13, parents[11])
        self.assertEqual(11, parents[inserted])
        self.assertEqual(inserted, parents[7])
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_native_reroot_reads_new_parent_after_insertion_commits(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        insertion = self.edge_insertion(11, 13)
        native = Client()
        native.cookies.update(self.client.cookies)

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                inserted = self.result(self.client.post(
                        '/%d/treenode/insert' % self.test_project_id, insertion))['treenode_id']
                waiting = self.overlapping_request(pool, lambda: native.post(
                        '/%d/skeleton/reroot' % self.test_project_id, {'treenode_id': 13}))
                self.assertFalse(waiting.done(), 'Native reroot did not wait for insertion')
            response = waiting.result(timeout=10)

        self.assertStatus(response)
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        self.assertEqual(original_nodes | {inserted}, set(parents))
        self.assertIsNone(parents[13])
        self.assertEqual(13, parents[inserted])
        self.assertEqual(inserted, parents[11])
        self.assertEqual(11, parents[7])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=75)
    def test_reroot_node_lock_timeout_rolls_back_annotation_and_parents(self):
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        change = self.change(self.annotation_a, 13)
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('lock_timeout')")
            previous_timeout = cursor.fetchone()[0]
            # Bound the unfixed UPDATE too, so this regression cannot hang.
            cursor.execute("SELECT set_config('lock_timeout', '400ms', false)")
        try:
            with self.held_node_lock(7):
                response = self.post(change)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('lock_timeout', %s, false)", [previous_timeout])

        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual({'code': 'edit_lock_timeout'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        self.assertEqual(parents,
                dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id')))
        self.assertEqual(7, self.root(1))

        self.assertEqual({'skeleton_id': 1, 'root_id': 13}, self.result(self.post(change)))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_reroot_locks_nodes_discovered_after_waiting_for_append(self):
        original_nodes = set(Treenode.objects.filter(skeleton_id=1).values_list('id', flat=True))
        # Ordinary appends do not take the topology lock used by edge insertion.
        root = Treenode.objects.get(pk=7)
        create_url = '/%d/treenode/create' % self.test_project_id
        first_append = {
            'parent_id': root.id, 'state': make_nocheck_state(),
            'x': root.location_x + 1, 'y': root.location_y, 'z': root.location_z,
        }
        change = self.change(self.annotation_a, 13)
        matching = Client()
        matching.cookies.update(self.client.cookies)
        native = Client()
        native.cookies.update(self.client.cookies)
        append_ready = Event()
        release_append = Event()
        appends = []
        append_futures = []
        inserted = None

        with ThreadPoolExecutor(max_workers=2) as pool:
            def hold_native_append():
                try:
                    with transaction.atomic():
                        response = native.post(create_url, {
                            **first_append,
                            'parent_id': inserted,
                            'y': first_append['y'] + 1,
                        })
                        appends.append(response)
                        append_ready.set()
                        if not release_append.wait(timeout=10):
                            raise RuntimeError('The test did not release the native append')
                    return response
                finally:
                    connections['default'].close()

            def append_at_fresh_graph_read(execute, sql, params, many, context):
                if (not append_futures and sql.lstrip().startswith('SELECT')
                        and 'FROM "treenode"' in sql and '"skeleton_id" =' in sql
                        and '"parent_id"' in sql and '"confidence"' in sql):
                    append_futures.append(pool.submit(hold_native_append))
                    if not append_ready.wait(timeout=5):
                        raise RuntimeError('Native append did not acquire the newly inserted node')
                return execute(sql, params, many, context)

            def match_while_append_holds_new_node():
                # The unfixed final UPDATE must time out rather than hang.
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('lock_timeout', '1s', false)")
                with connection.execute_wrapper(append_at_fresh_graph_read):
                    return self.post(change, client=matching)

            try:
                with transaction.atomic():
                    inserted = self.result(self.client.post(
                            create_url, first_append))['treenode_id']
                    waiting = self.overlapping_request(pool, match_while_append_holds_new_node)
                    self.assertFalse(waiting.done(), 'Reroot did not wait for the first append')
                response = waiting.result(timeout=10)
            finally:
                release_append.set()
                for future in append_futures:
                    future.result(timeout=10)

        self.assertEqual(1, len(appends), 'The test did not contend on the newly inserted node')
        appended = self.result(appends[0])['treenode_id']
        self.assertEqual(409, response.status_code, response.content)
        self.assertEqual({'code': 'edit_lock_timeout'}, response.json()['meta'])
        self.assertEqual(set(), self.neurons_with(self.annotation_a))
        parents = dict(Treenode.objects.filter(skeleton_id=1).values_list('id', 'parent_id'))
        self.assertEqual(original_nodes | {inserted, appended}, set(parents))
        self.assertIsNone(parents[7])
        self.assertEqual(7, parents[inserted])
        self.assertEqual(7, parents[11])
        self.assertEqual(11, parents[13])
        self.assertEqual(inserted, parents[appended])
