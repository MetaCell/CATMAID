# -*- coding: utf-8 -*-

import json
import uuid
from unittest.mock import patch

from catmaid.control import annotation
from catmaid.models import ClassInstanceClassInstance, Treenode

from .common import CatmaidApiTestCase


SKELETON_1_NEURON = 2
SKELETON_373_NEURON = 374


def task_annotation(study_id):
    return 'neuroglass:study:%s:task:%s' % (study_id, uuid.uuid4())


class TaskLinksApiTests(CatmaidApiTestCase):
    def setUp(self):
        super().setUp()
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
        return json.loads(response.content.decode('utf-8'))['results']

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
