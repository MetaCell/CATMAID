"""Structural edits preserve immutable SWC source identifiers."""

import json
from collections import Counter
from contextlib import contextmanager
from threading import Barrier, Event, Thread
import time
from types import SimpleNamespace
from unittest.mock import patch

import networkx as nx

from django.db import close_old_connections, connections, transaction
from django.db.models import F

from catmaid.control.annotation import _annotate_entities, annotations_for_skeleton
from catmaid.control.authentication import PermissionError
from catmaid.control.neuron import _delete_neuron_and_unused_annotations
from catmaid.control.skeleton import (
    _get_neuronname_from_skeletonid, _import_skeleton, _join_skeleton,
)
from catmaid.models import ClassInstance, ClassInstanceClassInstance, Treenode, User
from catmaid.source_fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX
from catmaid.state import make_nocheck_state

from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


class SourceFragmentFixtures:
    def setUp(self):
        super().setUp()
        self.client.force_login(self.test_user)

    def fragment(self, name, identifiers):
        graph = nx.DiGraph()
        for index in range(3):
            graph.add_node(index, x=index * 10.0, y=0.0, z=0.0, radius=1.0)
        graph.add_edges_from(((0, 1), (1, 2)))
        with transaction.atomic():
            imported = _import_skeleton(self.test_user, self.test_project_id,
                    graph, name=name, annotations=[])
            annotations = {SOURCE_FRAGMENT_ANNOTATION_PREFIX + identifier:
                    {'user_id': self.test_user_id} for identifier in identifiers}
            _annotate_entities(self.test_project_id, [imported['neuron_id']],
                    annotations, allow_reserved=True)
        return SimpleNamespace(
            neuron=imported['neuron_id'], skeleton=imported['skeleton_id'],
            nodes=[graph.nodes[index]['id'] for index in range(3)], name=name)

    def source_annotations(self, neuron_id):
        return list(ClassInstanceClassInstance.objects.filter(
            project_id=self.test_project_id, class_instance_a_id=neuron_id,
            relation__relation_name='annotated_with',
            class_instance_b__class_column__class_name='annotation',
            class_instance_b__name__startswith=SOURCE_FRAGMENT_ANNOTATION_PREFIX
        ).values_list('class_instance_b__name', 'user_id'))

    def assert_sources(self, neuron_id, identifiers):
        annotations = self.source_annotations(neuron_id)
        self.assertEqual(Counter(name for name, uid in annotations),
                Counter(SOURCE_FRAGMENT_ANNOTATION_PREFIX + item for item in identifiers))
        self.assertTrue(all(uid == self.test_user_id for name, uid in annotations))

    def join(self, winner, loser, annotation_map=None, omit_map=False):
        data = {'from_id': winner.nodes[0], 'to_id': loser.nodes[0]}
        if not omit_map:
            data['annotation_set'] = json.dumps(annotation_map or {})
        return self.client.post(f'/{self.test_project_id}/skeleton/join', data)


class SourceFragmentSkeletonTests(SourceFragmentFixtures, CatmaidApiTestCase):
    def test_merge_unions_literals_deduplicates_and_preserves_winner_name(self):
        winner = self.fragment('Winning visible label', ['00012', 'part{n}', 'comma,id'])
        loser = self.fragment('Losing visible label', ['00012', '900719925474099312345'])
        _annotate_entities(self.test_project_id, [winner.neuron],
                {'keep': {'user_id': self.test_user_id}})
        _annotate_entities(self.test_project_id, [loser.neuron],
                {'discard': {'user_id': self.test_user_id}})
        response = self.join(winner, loser, {
            'keep': self.test_user_id,
            SOURCE_FRAGMENT_ANNOTATION_PREFIX + '00012': 1,
        })
        self.assertStatus(response)
        self.assertEqual(response.json()['result_skeleton_id'], winner.skeleton)
        self.assert_sources(winner.neuron,
                ['00012', 'part{n}', 'comma,id', '900719925474099312345'])
        self.assertEqual(ClassInstance.objects.get(pk=winner.neuron).name, winner.name)
        self.assertFalse(ClassInstance.objects.filter(pk=loser.neuron).exists())
        self.assertEqual(set(annotations_for_skeleton(self.test_project_id,
                winner.skeleton)), {
                    'keep', 'source-fragment:00012', 'source-fragment:part{n}',
                    'source-fragment:comma,id', 'source-fragment:900719925474099312345',
                })

    def test_empty_and_omitted_maps_preserve_chained_contributors(self):
        winner = self.fragment('A', ['A'])
        loser = self.fragment('B', ['B'])
        third = self.fragment('C', ['C'])
        self.assertStatus(self.join(winner, loser))
        self.assertStatus(self.join(winner, third, omit_map=True))
        self.assert_sources(winner.neuron, ['A', 'B', 'C'])
        annotation = ClassInstance.objects.get(project_id=self.test_project_id,
                class_column__class_name='annotation', name='source-fragment:B')
        response = self.client.post(f'/{self.test_project_id}/annotations/query-targets', {
            'types[0]': 'neuron', 'annotated_with': str(annotation.id),
        })
        self.assertStatus(response)
        self.assertEqual([(entity['id'], entity['name'], entity['skeleton_ids'])
                for entity in response.json()['entities']],
                [(winner.neuron, 'A', [winner.skeleton])])

    def test_stable_partner_is_actual_named_winner(self):
        requested_winner = self.fragment('Requested', ['A'])
        stable_winner = self.fragment('Stable visible label', ['B'])
        _annotate_entities(self.test_project_id, [stable_winner.neuron],
                {'stable': {'user_id': self.test_user_id}})
        response = self.join(requested_winner, stable_winner)
        self.assertStatus(response)
        self.assertTrue(response.json()['stable_annotation_swap'])
        self.assertEqual(response.json()['result_skeleton_id'], stable_winner.skeleton)
        self.assert_sources(stable_winner.neuron, ['A', 'B'])
        self.assertEqual(ClassInstance.objects.get(pk=stable_winner.neuron).name,
                stable_winner.name)

    def test_split_copies_all_identifiers_even_with_empty_downstream_map(self):
        original = self.fragment('Original visible label', ['0001', 'part{n}', 'A,B'])
        response = self.client.post(f'/{self.test_project_id}/skeleton/split', {
            'treenode_id': original.nodes[1],
            'upstream_annotation_map': '{}', 'downstream_annotation_map': '{}',
        })
        self.assertStatus(response)
        new_skeleton = response.json()['new_skeleton_id']
        new_neuron = _get_neuronname_from_skeletonid(self.test_project_id, new_skeleton)
        for neuron_id in (original.neuron, new_neuron['neuronid']):
            self.assert_sources(neuron_id, ['0001', 'part{n}', 'A,B'])
        self.assertEqual(ClassInstance.objects.get(pk=original.neuron).name, original.name)
        self.assertEqual(new_neuron['neuronname'], f"Neuron {new_neuron['neuronid']}")
        # Reuniting split partners does not duplicate their shared source IDs.
        response = self.client.post(f'/{self.test_project_id}/skeleton/join', {
            'from_id': original.nodes[0], 'to_id': original.nodes[1],
            'annotation_set': '{}',
        })
        self.assertStatus(response)
        self.assert_sources(original.neuron, ['0001', 'part{n}', 'A,B'])

    def test_fabricated_reserved_identifier_is_forbidden_in_merge_and_split(self):
        winner = self.fragment('A', ['A'])
        loser = self.fragment('B', ['B'])
        forged = {'source-fragment:forged': self.test_user_id}
        response = self.join(winner, loser, forged)
        self.assertStatus(response, 403)
        self.assertEqual(Treenode.objects.get(pk=loser.nodes[0]).skeleton_id,
                loser.skeleton)
        response = self.client.post(f'/{self.test_project_id}/skeleton/split', {
            'treenode_id': winner.nodes[1],
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': json.dumps(forged),
        })
        self.assertStatus(response, 403)
        self.assertEqual(Treenode.objects.get(pk=winner.nodes[1]).skeleton_id,
                winner.skeleton)
        self.assert_sources(winner.neuron, ['A'])
        self.assert_sources(loser.neuron, ['B'])

    def test_native_import_cannot_create_reserved_metadata(self):
        graph = nx.DiGraph()
        graph.add_node(0, x=0, y=0, z=0)
        before = ClassInstance.objects.count()
        with self.assertRaises(PermissionError):
            _import_skeleton(self.test_user, self.test_project_id, graph,
                    annotations=['source-fragment:forged'])
        self.assertEqual(ClassInstance.objects.count(), before)


class SourceFragmentTransactionTests(SourceFragmentFixtures, CatmaidApiTransactionTestCase):
    def test_whole_neuron_deletion_still_works(self):
        fragment = self.fragment('Delete complete entity', ['A'])
        annotation = ClassInstance.objects.get(project_id=self.test_project_id,
                class_column__class_name='annotation', name='source-fragment:A')
        response = self.client.post(
                f'/{self.test_project_id}/neuron/{fragment.neuron}/delete')
        self.assertStatus(response)
        self.assertFalse(ClassInstance.objects.filter(pk=fragment.neuron).exists())
        self.assertFalse(Treenode.objects.filter(skeleton_id=fragment.skeleton).exists())
        self.assertFalse(ClassInstance.objects.filter(pk=annotation.id).exists())

    def test_deletion_preserves_shared_annotations_until_their_last_use(self):
        first = self.fragment('First', ['shared', 'first-only'])
        second = self.fragment('Second', ['shared'])
        _annotate_entities(self.test_project_id, [first.neuron, second.neuron],
                {'shared-label': {'user_id': self.test_user_id}})
        _annotate_entities(self.test_project_id, [first.neuron],
                {'private-label': {'user_id': self.test_user_id}})
        self.assertStatus(self.client.post(
                f'/{self.test_project_id}/neuron/{first.neuron}/delete'))
        self.assert_sources(second.neuron, ['shared'])
        names = ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation').values_list('name', flat=True)
        self.assertIn('shared-label', names)
        self.assertNotIn('private-label', names)
        self.assertNotIn('source-fragment:first-only', names)
        self.assertStatus(self.client.post(
                f'/{self.test_project_id}/neuron/{second.neuron}/delete'))
        self.assertFalse(ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation',
                name__in=['shared-label', 'source-fragment:shared']).exists())

    def test_deleting_the_final_treenode_removes_unused_provenance(self):
        fragment = self.fragment('Delete nodes', ['A'])
        for node_id in reversed(fragment.nodes):
            response = self.client.post(f'/{self.test_project_id}/treenode/delete',
                    {'treenode_id': node_id, 'state': make_nocheck_state()})
            self.assertStatus(response)
        self.assertFalse(ClassInstance.objects.filter(pk=fragment.neuron).exists())
        self.assertFalse(ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation', name='source-fragment:A').exists())

    @contextmanager
    def paused_join(self, winner, loser):
        ready, release = Event(), Event()
        failures = []

        def hold_join():
            close_old_connections()
            try:
                with transaction.atomic():
                    user = User.objects.get(pk=self.test_user_id)
                    _join_skeleton(user, winner.nodes[0], loser.nodes[0],
                            self.test_project_id, {})
                    ready.set()
                    if not release.wait(15):
                        raise AssertionError('Timed out waiting to commit first join')
            except BaseException as error:
                failures.append(error)
            finally:
                ready.set()
                connections['default'].close()

        worker = Thread(target=hold_join, daemon=True)
        worker.start()
        try:
            self.assertTrue(ready.wait(15), 'First join did not acquire locks')
            if failures:
                raise failures[0]
            yield
        finally:
            release.set()
            worker.join(15)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])

    def test_concurrent_joins_at_different_nodes_retry_without_losing_identifiers(self):
        winner = self.fragment('A', ['A'])
        first = self.fragment('B', ['B'])
        second = self.fragment('C', ['C'])
        with self.paused_join(winner, first):
            with self.assertRaisesRegex(ValueError, 'being edited; retry'):
                with transaction.atomic():
                    _join_skeleton(self.test_user, winner.nodes[2], second.nodes[0],
                            self.test_project_id, {})
        with transaction.atomic():
            _join_skeleton(self.test_user, winner.nodes[2], second.nodes[0],
                    self.test_project_id, {})
        self.assert_sources(winner.neuron, ['A', 'B', 'C'])
        self.assertEqual(ClassInstance.objects.get(pk=winner.neuron).name, 'A')

    def test_split_during_join_retries_with_complete_node_and_identifier_sets(self):
        winner = self.fragment('A', ['A'])
        loser = self.fragment('B', ['B'])
        data = {'treenode_id': winner.nodes[1],
                'upstream_annotation_map': '{}', 'downstream_annotation_map': '{}'}
        with self.paused_join(winner, loser):
            response = self.client.post(f'/{self.test_project_id}/skeleton/split', data)
            self.assertStatus(response, 400)
            self.assertIn('being edited; retry', response.json()['error'])
        response = self.client.post(f'/{self.test_project_id}/skeleton/split', data)
        self.assertStatus(response)
        new_skeleton = response.json()['new_skeleton_id']
        new_neuron = _get_neuronname_from_skeletonid(self.test_project_id, new_skeleton)
        self.assert_sources(winner.neuron, ['A', 'B'])
        self.assert_sources(new_neuron['neuronid'], ['A', 'B'])
        crossing_parents = Treenode.objects.filter(
                project_id=self.test_project_id,
                skeleton_id__in=[winner.skeleton, new_skeleton]
            ).exclude(parent__isnull=True).exclude(parent__skeleton_id=F('skeleton_id'))
        self.assertFalse(crossing_parents.exists())


class SourceFragmentCleanupConcurrencyTests(SourceFragmentFixtures, CatmaidApiTransactionTestCase):
    def run_overlapping(self, operations, *, ready=None, rollback_index=None):
        """Keep both transactions open until they finish work or contend in PG."""
        release = Event()
        completed = [Event(), Event()]
        backend_ids = [None, None]
        failures = []
        contended = False

        def run(index):
            close_old_connections()
            try:
                with transaction.atomic():
                    with connections['default'].cursor() as cursor:
                        cursor.execute("SELECT set_config('lock_timeout', '10s', true), "
                                "set_config('statement_timeout', '15s', true)")
                        cursor.execute('SELECT pg_backend_pid()')
                        backend_ids[index] = cursor.fetchone()[0]
                    operations[index]()
                    completed[index].set()
                    if not release.wait(15):
                        raise AssertionError('Timed out waiting to finish cleanup transaction')
                    if index == rollback_index:
                        transaction.set_rollback(True)
            except BaseException as error:
                failures.append(error)
            finally:
                connections['default'].close()

        workers = [Thread(target=run, args=(index,), daemon=True) for index in range(2)]
        for worker in workers:
            worker.start()
        try:
            if ready is not None:
                self.assertTrue(ready.wait(10), 'Both neuron deletions did not reach cleanup')
            deadline = time.monotonic() + 10
            while not all(event.is_set() for event in completed):
                if failures:
                    raise failures[0]
                if all(identifier is not None for identifier in backend_ids):
                    with connections['default'].cursor() as cursor:
                        cursor.execute('SELECT pg_blocking_pids(%s), pg_blocking_pids(%s)',
                                backend_ids)
                        first, second = cursor.fetchone()
                    if backend_ids[1] in first or backend_ids[0] in second:
                        contended = True
                        break
                self.assertLess(time.monotonic(), deadline, 'Transactions did not overlap')
                release.wait(0.01)
        finally:
            release.set()
            for worker in workers:
                worker.join(15)
        self.assertFalse(any(worker.is_alive() for worker in workers))
        if failures:
            raise failures[0]
        return contended

    def delete_concurrently(self, neuron_ids, *, rollback_index=None):
        """Force both link cascades before either last-use check can finish."""
        deleted = Event()
        barrier = Barrier(2, action=deleted.set, timeout=10)
        original_delete = ClassInstance.delete

        def delete_and_pause(instance, *args, **kwargs):
            identifier = instance.pk
            result = original_delete(instance, *args, **kwargs)
            if identifier in neuron_ids:
                barrier.wait()
            return result

        operations = [lambda identifier=identifier:
                _delete_neuron_and_unused_annotations(ClassInstance.objects.get(pk=identifier))
                for identifier in neuron_ids]
        with patch.object(ClassInstance, 'delete', delete_and_pause):
            return self.run_overlapping(operations, ready=deleted,
                    rollback_index=rollback_index)

    def split(self, fragment):
        response = self.client.post(f'/{self.test_project_id}/skeleton/split', {
            'treenode_id': fragment.nodes[1],
            'upstream_annotation_map': '{}', 'downstream_annotation_map': '{}',
        })
        self.assertStatus(response)
        skeleton_id = response.json()['new_skeleton_id']
        return _get_neuronname_from_skeletonid(self.test_project_id,
                skeleton_id)['neuronid']

    def assert_annotation_absent(self, identifiers):
        self.assertFalse(ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation',
                name__in=[SOURCE_FRAGMENT_ANNOTATION_PREFIX + item
                          for item in identifiers]).exists())

    def test_concurrent_deletion_of_split_results_removes_orphan_provenance(self):
        original = self.fragment('Split source', ['shared'])
        second_neuron = self.split(original)
        contended = self.delete_concurrently([original.neuron, second_neuron])
        self.assertFalse(ClassInstance.objects.filter(
                pk__in=[original.neuron, second_neuron]).exists())
        # This is the same name/class lookup used to reject incoming fragments.
        self.assert_annotation_absent(['shared'])
        self.assertTrue(contended, 'Shared annotation cleanup did not serialize')

    def test_multiple_shared_annotations_lock_in_one_order(self):
        first = self.fragment('First', ['a', 'b', 'c'])
        second = self.fragment('Second', ['c', 'b', 'a'])
        _annotate_entities(self.test_project_id, [first.neuron, second.neuron],
                {'shared-label': {'user_id': self.test_user_id}})
        self.assertTrue(self.delete_concurrently([first.neuron, second.neuron]))
        self.assert_annotation_absent(['a', 'b', 'c'])
        self.assertFalse(ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation', name='shared-label').exists())

    def test_rollback_retains_the_surviving_neurons_provenance(self):
        first = self.fragment('Committed deletion', ['shared'])
        second = self.fragment('Rolled back deletion', ['shared'])
        self.assertTrue(self.delete_concurrently([first.neuron, second.neuron],
                rollback_index=1))
        self.assertFalse(ClassInstance.objects.filter(pk=first.neuron).exists())
        self.assertTrue(ClassInstance.objects.filter(pk=second.neuron).exists())
        self.assert_sources(second.neuron, ['shared'])

    def test_cleanup_is_atomic_when_called_without_an_outer_transaction(self):
        fragment = self.fragment('Direct cleanup', ['unshared'])
        self.assertFalse(connections['default'].in_atomic_block)
        _delete_neuron_and_unused_annotations(ClassInstance.objects.get(pk=fragment.neuron))
        self.assert_annotation_absent(['unshared'])

    def during_cleanup(self, fragment, operation):
        cleaned = Event()

        def cleanup():
            _delete_neuron_and_unused_annotations(
                    ClassInstance.objects.get(pk=fragment.neuron))
            cleaned.set()

        def edit():
            if not cleaned.wait(10):
                raise AssertionError('Cleanup did not acquire the annotation locks')
            operation()

        self.run_overlapping([cleanup, edit])

    def test_split_retains_shared_provenance_during_another_neurons_cleanup(self):
        removed = self.fragment('Removed', ['shared'])
        retained = self.fragment('Split survivor', ['shared', 'retained'])
        results = []
        self.during_cleanup(removed, lambda: results.append(self.split(retained)))
        self.assert_sources(retained.neuron, ['shared', 'retained'])
        self.assert_sources(results[0], ['shared', 'retained'])

    def test_join_retains_shared_provenance_during_another_neurons_cleanup(self):
        removed = self.fragment('Removed', ['shared'])
        winner = self.fragment('Winner', ['shared', 'winner'])
        loser = self.fragment('Loser', ['loser'])

        def join():
            user = User.objects.get(pk=self.test_user_id)
            _join_skeleton(user, winner.nodes[0], loser.nodes[0],
                    self.test_project_id, {})

        self.during_cleanup(removed, join)
        self.assert_sources(winner.neuron, ['shared', 'winner', 'loser'])
