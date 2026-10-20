"""Structural edits preserve immutable SWC source identifiers."""

import json
from collections import Counter
from contextlib import contextmanager
from threading import Event, Thread
from types import SimpleNamespace

import networkx as nx

from django.db import close_old_connections, connections, transaction
from django.db.models import F

from catmaid.control.annotation import _annotate_entities, annotations_for_skeleton
from catmaid.control.authentication import PermissionError
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
