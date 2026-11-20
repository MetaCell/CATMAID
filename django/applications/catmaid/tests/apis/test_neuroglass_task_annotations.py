"""NeuroGlass task links remain on the original skeleton after a native split."""

import json
import uuid

from django.db import transaction

from catmaid.control import annotation
from catmaid.source_fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX

from .common import CatmaidApiTestCase
from .test_annotation_matching import AnnotationMatchingFixtures, SKELETON_1_NEURON


class TaskSplitPlacementTests(AnnotationMatchingFixtures, CatmaidApiTestCase):
    def setUp(self):
        super().setUp()
        self.study_id = str(uuid.uuid4())
        self.annotation_a = f"neuroglass:study:{self.study_id}:task:{uuid.uuid4()}"
        self.annotation_b = f"neuroglass:study:{self.study_id}:task:{uuid.uuid4()}"

    def test_fast_split_keeps_task_link_on_the_original_skeleton(self):
        self.assertEqual(1, self.result(self.post(self.change(self.annotation_a, 7)))['skeleton_id'])
        provenance = SOURCE_FRAGMENT_ANNOTATION_PREFIX + 'imported-fragment'
        annotation._annotate_entities(self.test_project_id, [SKELETON_1_NEURON],
                {name: {'user_id': self.test_user_id}
                    for name in ('ordinary annotation', 'neuroglass:study:notes:task:label', provenance)}, allow_reserved=True)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id,
                {'treenode_id': 11}))

        self.assertEqual(1, result['existing_skeleton_id'])
        new_skeleton = result['new_skeleton_id']
        self.assertNotEqual(1, new_skeleton)
        self.assertEqual(7, self.root(1))
        self.assertEqual(11, self.root(new_skeleton))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        for skeleton_id in (1, new_skeleton):
            self.assertLessEqual({'ordinary annotation', 'neuroglass:study:notes:task:label', provenance},
                    annotation.annotations_for_skeleton(self.test_project_id, skeleton_id).keys())

    def test_confirmed_split_does_not_duplicate_the_task_link(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)
        self.link('shared label', SKELETON_1_NEURON)
        selected = json.dumps({self.annotation_a: self.test_user_id, 'shared label': self.test_user_id})

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': selected,
            'downstream_annotation_map': selected,
        }))

        self.assertEqual(1, result['existing_skeleton_id'])
        self.assertEqual(7, self.root(1))
        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))
        self.assertEqual({'shared label': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))

    def test_confirmed_split_keeps_task_link_when_all_ordinary_labels_go_to_new_skeleton(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)
        self.link('selected label', SKELETON_1_NEURON)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': json.dumps({
                self.annotation_a: self.test_user_id,
                self.annotation_b: self.test_user_id,
                'selected label': self.test_user_id,
            }),
        }))

        self.assertEqual({self.annotation_a: self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, 1))
        self.assertEqual({'selected label': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))
        self.assertEqual(set(), self.neurons_with(self.annotation_b))

    def test_confirmed_split_keeps_task_link_omitted_from_both_maps(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)

        self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': '{}',
            'downstream_annotation_map': '{}',
        }))

        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(self.annotation_a))

    def test_split_cannot_add_task_links_supplied_in_either_annotation_map(self):
        self.link('ordinary label', SKELETON_1_NEURON)
        selected = json.dumps({
            self.annotation_b: self.test_user_id,
            'ordinary label': self.test_user_id,
        })

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': selected,
            'downstream_annotation_map': selected,
        }))

        self.assertEqual(set(), self.neurons_with(self.annotation_b))
        for skeleton_id in (1, result['new_skeleton_id']):
            self.assertEqual({'ordinary label': self.test_user_id},
                    annotation.annotations_for_skeleton(self.test_project_id, skeleton_id))

    def test_split_cannot_duplicate_a_task_link_with_a_counting_template(self):
        task_prefix = f"neuroglass:study:{self.study_id}:task:00000000-0000-0000-0000-"
        name = task_prefix + '000000000001'
        self.link(name, SKELETON_1_NEURON)
        self.link('ordinary label', SKELETON_1_NEURON)

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': json.dumps({'ordinary label': self.test_user_id}),
            'downstream_annotation_map': json.dumps({task_prefix + '00000000000{n1}': self.test_user_id}),
        }))

        self.assertEqual({SKELETON_1_NEURON}, self.neurons_with(name))
        self.assertEqual({}, annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))

    def test_split_cannot_create_task_links_from_counting_templates_in_either_map(self):
        task_prefix = f"neuroglass:study:{self.study_id}:task:00000000-0000-0000-0000-"
        self.link('ordinary label', SKELETON_1_NEURON)
        cases = [
            (task_prefix + '00000000000{n}', task_prefix + '000000000001'),
            (task_prefix + '00000000000{n1}', task_prefix + '000000000001'),
            (task_prefix + '0000000000{n42}', task_prefix + '000000000042'),
            (task_prefix + '00000000000{n0001}', task_prefix + '000000000001'),
            (task_prefix + '00000000000{n{n1}}', task_prefix + '000000000001'),
            ('neuroglass:study:00000000-0000-0000-0000-00000000000{n}:task:00000000-0000-0000-0000-000000000002',
             'neuroglass:study:00000000-0000-0000-0000-000000000001:task:00000000-0000-0000-0000-000000000002'),
        ]
        for template, name in cases:
            with self.subTest(template=template), transaction.atomic():
                selected = json.dumps({template: self.test_user_id, 'ordinary label': self.test_user_id})
                result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
                    'treenode_id': 11,
                    'upstream_annotation_map': selected,
                    'downstream_annotation_map': selected,
                }))

                self.assertEqual(set(), self.neurons_with(name))
                for skeleton_id in (1, result['new_skeleton_id']):
                    self.assertEqual({'ordinary label': self.test_user_id},
                            annotation.annotations_for_skeleton(self.test_project_id, skeleton_id))
                transaction.set_rollback(True)

    def test_split_still_expands_ordinary_annotation_counting_templates(self):
        self.link(self.annotation_a, SKELETON_1_NEURON)
        self.link('ordinary label', SKELETON_1_NEURON)
        selected = json.dumps({'ordinary label': self.test_user_id, 'part-{n2}': self.test_user_id})

        result = self.result(self.client.post('/%d/skeleton/split' % self.test_project_id, {
            'treenode_id': 11,
            'upstream_annotation_map': selected,
            'downstream_annotation_map': selected,
        }))

        self.assertEqual({
            self.annotation_a: self.test_user_id,
            'ordinary label': self.test_user_id,
            'part-2': self.test_user_id,
        }, annotation.annotations_for_skeleton(self.test_project_id, 1))
        self.assertEqual({'ordinary label': self.test_user_id, 'part-2': self.test_user_id},
                annotation.annotations_for_skeleton(self.test_project_id, result['new_skeleton_id']))
