"""Provenance annotations remain literal, searchable and importer-owned."""

from catmaid.control.annotation import (
    _annotate_entities, _update_neuron_annotations, annotations_for_skeleton,
    clear_annotations,
)
from catmaid.control.authentication import PermissionError
from catmaid.control.skeleton import _import_skeleton_swc
from catmaid.models import Class, ClassInstance, ClassInstanceClassInstance, Relation, Treenode, Project

from .common import CatmaidApiTestCase


class SourceFragmentAnnotationApiTests(CatmaidApiTestCase):
    neuron_id = 2365
    skeleton_id = 2364

    def setUp(self):
        super().setUp()
        self.client.force_login(self.test_user)

    def seed(self, *names, neuron_id=None):
        neuron_id = self.neuron_id if neuron_id is None else neuron_id
        objects, _, _ = _annotate_entities(
            self.test_project_id, [neuron_id],
            {name: {'user_id': self.test_user_id} for name in names},
            allow_reserved=True,
        )
        return {obj.name: obj.id for obj in objects}

    def snapshot(self):
        return (
            list(ClassInstance.objects.filter(project_id=self.test_project_id,
                class_column__class_name='annotation').order_by('id').values_list('id', 'name')),
            list(ClassInstanceClassInstance.objects.filter(project_id=self.test_project_id,
                relation__relation_name='annotated_with').order_by('id').values_list(
                    'id', 'class_instance_a_id', 'class_instance_b_id', 'user_id')),
        )

    def refused(self, path, data):
        before = self.snapshot()
        response = self.client.post(f'/{self.test_project_id}/{path}', data)
        self.assertEqual(response.status_code, 403, response.content)
        self.assertIn('importer-owned', response.json()['error'])
        self.assertEqual(self.snapshot(), before)
        return response

    def test_manual_creation_and_meta_creation_are_preflighted(self):
        for data in (
            {'annotations[0]': 'ordinary', 'annotations[1]': 'source-fragment:001',
             'entity_ids[0]': self.neuron_id},
            {'annotations[0]': 'ordinary', 'meta_annotations[0]': 'source-fragment:001',
             'entity_ids[0]': self.neuron_id},
        ):
            with self.subTest(data=data):
                self.refused('annotations/add', data)

    def test_manual_attachment_of_existing_fragment_and_meta_target_are_refused(self):
        reserved = 'source-fragment:001'
        aid = self.seed(reserved)[reserved]
        self.refused('annotations/add', {
            'annotations[0]': reserved, 'entity_ids[0]': 2381})
        self.refused('annotations/add', {
            'annotations[0]': 'ordinary-meta', 'entity_ids[0]': aid})

    def test_mixed_remove_is_atomic_and_single_remove_is_protected(self):
        ids = self.seed('ordinary', 'source-fragment:001')
        self.refused('annotations/remove', {
            'annotation_ids[0]': ids['ordinary'],
            'annotation_ids[1]': ids['source-fragment:001'],
            'entity_ids[0]': self.neuron_id})
        self.refused(f"annotations/{ids['source-fragment:001']}/remove", {
            'entity_ids[0]': self.neuron_id})

    def test_removing_meta_annotation_from_reserved_entity_is_refused(self):
        reserved_id = self.seed('source-fragment:001')['source-fragment:001']
        meta_id = self.seed('ordinary-meta', neuron_id=reserved_id)['ordinary-meta']
        self.refused('annotations/remove', {
            'annotation_ids[0]': meta_id, 'entity_ids[0]': reserved_id})

    def test_replace_is_atomic_for_reserved_add_remove_and_targets(self):
        ids = self.seed('ordinary', 'source-fragment:001')
        for data in (
            {'to_remove[0]': 'ordinary', 'to_add[0]': 'source-fragment:002',
             'target_ids[0]': self.neuron_id},
            {'to_remove[0]': 'source-fragment:001', 'to_add[0]': 'new-ordinary',
             'target_ids[0]': self.neuron_id},
            {'to_add[0]': 'ordinary-meta', 'target_ids[0]': ids['source-fragment:001']},
        ):
            with self.subTest(data=data):
                self.refused('annotations/replace', data)

    def test_neuron_name_annotation_cannot_create_reserved_annotation(self):
        response = self.client.post(
            f'/{self.test_project_id}/neurons/{self.neuron_id}/rename',
            {'name': 'source-fragment:visible-neuron-name'})
        self.assertEqual(response.status_code, 200, response.content)
        self.refused('annotations/add-neuron-names', {
            'entity_ids[0]': 2381, 'entity_ids[1]': self.neuron_id})

    def test_generic_rename_blocks_actual_annotations_only(self):
        ids = self.seed('ordinary', 'source-fragment:001')
        self.refused(f"neurons/{ids['ordinary']}/rename", {'name': 'source-fragment:forged'})
        self.refused(f"neurons/{ids['source-fragment:001']}/rename", {'name': 'ordinary-now'})
        response = self.client.post(f'/{self.test_project_id}/neurons/{self.neuron_id}/rename',
            {'name': 'source-fragment:neuron-name'})
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(ClassInstance.objects.get(pk=self.neuron_id).name, 'source-fragment:neuron-name')

    def test_bulk_rename_preflights_all_annotations(self):
        reserved_id = self.seed('source-fragment:001')['source-fragment:001']
        before = ClassInstance.objects.get(pk=self.neuron_id).name
        self.refused('neurons/rename', {
            'names[0][0]': self.neuron_id, 'names[0][1]': 'changed-neuron',
            'names[1][0]': reserved_id, 'names[1][1]': 'changed-annotation'})
        self.assertEqual(ClassInstance.objects.get(pk=self.neuron_id).name, before)

    def test_generic_neuron_deletion_cannot_delete_reserved_annotation(self):
        reserved_id = self.seed('source-fragment:001')['source-fragment:001']
        self.refused(f'neuron/{reserved_id}/delete', {})

    def test_classification_mutation_bypasses_are_refused(self):
        ids = self.seed('ordinary', 'source-fragment:001')
        annotation_class = Class.objects.get(project_id=self.test_project_id, class_name='annotation')
        path = f'classification/{self.test_project_id}/instance-operation'
        for data in (
            {'operation': 'rename_node', 'id': ids['source-fragment:001'], 'title': 'changed'},
            {'operation': 'rename_node', 'id': ids['ordinary'], 'title': 'source-fragment:forged'},
            {'operation': 'remove_node', 'id': ids['source-fragment:001'], 'rel': 'annotation'},
            {'operation': 'create_node', 'classid': annotation_class.id, 'objname': 'source-fragment:forged'},
            {'operation': 'create_node', 'classid': annotation_class.id, 'objname': 'ordinary-meta',
             'parentid': ids['source-fragment:001']},
        ):
            with self.subTest(data=data):
                self.refused(path, data)

    def test_classification_subtree_preflight_prevents_partial_deletion(self):
        ids = self.seed('ordinary-parent', 'source-fragment:001')
        ClassInstanceClassInstance.objects.create(
            user_id=self.test_user_id, project_id=self.test_project_id,
            class_instance_a_id=ids['source-fragment:001'], class_instance_b_id=ids['ordinary-parent'],
            relation=Relation.objects.get(project_id=self.test_project_id, relation_name='annotated_with'))
        self.refused(f'classification/{self.test_project_id}/instance-operation', {
            'operation': 'remove_node', 'id': ids['ordinary-parent'], 'rel': 'element'})

    def test_internal_helpers_require_explicit_trusted_transfer_and_keep_literals(self):
        for helper in (_annotate_entities, _update_neuron_annotations):
            before = self.snapshot()
            with self.assertRaises(PermissionError):
                helper(self.test_project_id,
                    [self.neuron_id] if helper is _annotate_entities else self.neuron_id,
                    {'source-fragment:{n}': {'user_id': self.test_user_id}})
            self.assertEqual(self.snapshot(), before)
        names = ['source-fragment:{n}', 'source-fragment:{n009}', 'ordinary-{n}']
        self.seed(*names)
        stored = set(annotations_for_skeleton(self.test_project_id, self.skeleton_id))
        self.assertTrue(set(names[:2]).issubset(stored))
        self.assertIn('ordinary-1', stored)
        with self.assertRaises(PermissionError):
            _update_neuron_annotations(self.test_project_id, self.neuron_id, {})

    def test_clear_annotations_preserves_reserved_links(self):
        reserved = 'source-fragment:0001,{n} '
        self.seed('ordinary', reserved)
        before = ClassInstanceClassInstance.objects.get(
            class_instance_a_id=self.neuron_id, class_instance_b__name=reserved)
        clear_annotations(self.test_project_id, self.skeleton_id)
        self.assertEqual(set(annotations_for_skeleton(self.test_project_id, self.skeleton_id)), {reserved})
        after = ClassInstanceClassInstance.objects.get(pk=before.pk)
        self.assertEqual(before.creation_time, after.creation_time)

    def test_native_overwrite_retains_provenance_without_tagging_new_native_imports(self):
        swc = '1 1 0 0 0 1 -1\n2 3 1 1 1 1 1\n'
        imported = _import_skeleton_swc(self.test_user, self.test_project_id, swc, annotations=['ordinary'])
        self.assertEqual(set(annotations_for_skeleton(self.test_project_id, imported['skeleton_id'])), {'ordinary'})
        reserved = 'source-fragment:0001'
        self.seed(reserved, neuron_id=imported['neuron_id'])
        updated = _import_skeleton_swc(self.test_user, self.test_project_id, swc,
            skeleton_id=imported['skeleton_id'], annotations=['replacement'],
            force=True, replace_annotations=True)
        self.assertEqual(updated['skeleton_id'], imported['skeleton_id'])
        self.assertEqual(set(annotations_for_skeleton(self.test_project_id, updated['skeleton_id'])),
            {reserved, 'replacement'})

    def test_native_import_rejects_forged_provenance_before_morphology_changes(self):
        before = Treenode.objects.count(), ClassInstance.objects.count()
        with self.assertRaises(PermissionError):
            _import_skeleton_swc(self.test_user, self.test_project_id,
                '1 1 0 0 0 1 -1\n', annotations=['ordinary', 'source-fragment:forged'])
        self.assertEqual((Treenode.objects.count(), ClassInstance.objects.count()), before)

    def test_normalized_fragment_lookup_resolves_current_skeleton(self):
        names = [
            'source-fragment:000000900719925474099312345',
            'source-fragment:part,001', 'source-fragment:Test_100%',
            r'source-fragment:path\name', 'source-fragment:雪{n}',
            'source-fragment:internal spaces',
        ]
        ids = self.seed(*names)
        for name in names:
            with self.subTest(name=name):
                response = self.client.post(f'/{self.test_project_id}/annotations/query-targets', {
                    'types[0]': 'annotation', 'name': name.replace('\\', '\\\\'),
                    'name_exact': True, 'name_case_sensitive': True})
                self.assertEqual(response.status_code, 200, response.content)
                self.assertEqual([(item['id'], item['name']) for item in response.json()['entities']],
                    [(ids[name], name)])
                response = self.client.post(f'/{self.test_project_id}/annotations/query-targets', {
                    'types[0]': 'neuron', 'annotated_with[0]': ids[name], 'annotation_reference': 'id'})
                self.assertEqual(response.status_code, 200, response.content)
                self.assertEqual(response.json()['entities'][0]['skeleton_ids'], [self.skeleton_id])
        response = self.client.post(f'/{self.test_project_id}/annotations/query-targets', {
            'types[0]': 'annotation', 'name': 'source-fragment:test_100%',
            'name_exact': True, 'name_case_sensitive': True})
        self.assertEqual(response.json()['entities'], [])

    def test_existing_partial_case_insensitive_and_regex_search_modes_remain(self):
        ids = self.seed('ordinary-alpha', 'ordinary-beta')
        for query in (
            {'name': 'ALPHA'},
            {'name': ' ordinary-alpha ', 'name_exact': True, 'name_case_sensitive': True},
            {'name': '/ordinary-(alpha|beta)$', 'name_case_sensitive': True},
            {'name': ' /ordinary-(alpha|beta)$ ', 'name_exact': True, 'name_case_sensitive': True},
        ):
            response = self.client.post(f'/{self.test_project_id}/annotations/query-targets',
                {'types[0]': 'annotation', **query})
            self.assertEqual(response.status_code, 200, response.content)
            expected = set(ids.values()) if query['name'].strip().startswith('/') else {ids['ordinary-alpha']}
            self.assertEqual({item['id'] for item in response.json()['entities']}, expected)

    def test_audit_batch_returns_unmodified_identifiers_and_read_permissions_hold(self):
        names = ['source-fragment:0001', 'source-fragment:雪,{n} ']
        self.seed(*names)
        response = self.client.post(f'/{self.test_project_id}/annotations/forskeletons', {
            'skeleton_ids[0]': self.skeleton_id})
        self.assertEqual(response.status_code, 200, response.content)
        data = response.json()
        linked_ids = {str(item['id']) for item in data['skeletons'][str(self.skeleton_id)]}
        self.assertEqual({data['annotations'][aid] for aid in linked_ids}, set(names))
        self.client.logout()
        response = self.client.post(f'/{self.test_project_id}/annotations/query-targets', {
            'types[0]': 'annotation', 'name': names[0],
            'name_exact': True, 'name_case_sensitive': True})
        self.assertEqual(response.status_code, 403, response.content)

    def test_foreign_project_reserved_targets_cannot_bypass_namespace_guards(self):
        other = Project.objects.create(title='Other fragment project')
        other_class = Class.objects.create(user_id=self.test_user_id,
            project=other, class_name='annotation')
        foreign = ClassInstance.objects.create(user_id=self.test_user_id,
            project=other, class_column=other_class, name='source-fragment:foreign')
        local_class = Class.objects.get(project_id=self.test_project_id, class_name='annotation')
        for path, data in (
            ('annotations/add', {'annotations[0]': 'ordinary-meta', 'entity_ids[0]': foreign.id}),
            ('annotations/add-neuron-names', {'entity_ids[0]': foreign.id}),
            ('annotations/replace', {'to_add[0]': 'ordinary-meta', 'target_ids[0]': foreign.id}),
            (f'classification/{self.test_project_id}/instance-operation', {
                'operation': 'create_node', 'classid': local_class.id,
                'parentid': foreign.id, 'objname': 'ordinary-meta'}),
        ):
            with self.subTest(path=path):
                self.refused(path, data)
        self.assertFalse(ClassInstanceClassInstance.objects.filter(
            class_instance_a=foreign).exists())
