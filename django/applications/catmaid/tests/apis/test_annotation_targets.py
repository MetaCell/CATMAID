"""Atomic replacement of one annotation's complete target set."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event, Thread
from time import monotonic, sleep
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections, connection, connections, transaction
from django.test import Client, override_settings
from django.test.utils import CaptureQueriesContext

from catmaid.control import annotation
from catmaid.models import Class, ClassInstance, ClassInstanceClassInstance, Project, Relation
from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


class AnnotationTargetsMixin:
    name = 'annotation-targets-test'

    def setUp(self):
        super().setUp()
        self.fake_authentication()

    def label(self, target=2365, user_id=None):
        annotation._annotate_entities(self.test_project_id, [target], {
            self.name: {'user_id': user_id or self.test_user_id},
        })

    def create_annotation(self, name):
        return ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id, name=name,
            class_column=Class.objects.get(project_id=self.test_project_id, class_name='annotation'),
        )

    def targets(self):
        return set(ClassInstanceClassInstance.objects.filter(
            project_id=self.test_project_id, relation__relation_name='annotated_with',
            class_instance_b__name=self.name,
            class_instance_b__class_column__class_name='annotation',
        ).values_list('class_instance_a_id', flat=True))

    def set_targets(self, **data):
        return self.client.post(f'/{self.test_project_id}/annotations/set-targets', {
            'annotation': self.name, **data,
        })


class AnnotationTargetsApiTests(AnnotationTargetsMixin, CatmaidApiTestCase):
    def test_replaces_targets_without_changing_other_annotations(self):
        self.label()
        annotation._annotate_entities(self.test_project_id, [2365], {
            'keep-this-label': {'user_id': self.test_user_id},
        })
        response = self.set_targets(entity_ids=[2389])
        self.assertStatus(response)
        self.assertEqual(self.targets(), {2389})
        self.assertEqual(response.json()['entity_ids'], [2389])
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            class_instance_a_id=2365, class_instance_b__name='keep-this-label',
        ).exists())

    def test_skeleton_targets_resolve_to_neurons(self):
        self.label()
        skeleton_id = ClassInstanceClassInstance.objects.get(
            project_id=self.test_project_id, relation__relation_name='model_of',
            class_instance_b_id=2389,
        ).class_instance_a_id
        response = self.set_targets(skeleton_ids=[skeleton_id])
        self.assertStatus(response)
        self.assertEqual(self.targets(), {2389})
        self.assertEqual(response.json()['skeleton_ids'], [skeleton_id])

    def test_task_conflict_preserves_both_task_links(self):
        prefix = 'neuroglass:study:00000000-0000-0000-0000-000000000001:task:'
        owner_id = '00000000-0000-0000-0000-000000000002'
        owner = prefix + owner_id
        self.name = prefix + '00000000-0000-0000-0000-000000000003'
        self.label()
        self.assertStatus(self.set_targets(annotation=owner, entity_ids=[2389]))
        skeleton_id = ClassInstanceClassInstance.objects.get(
            project_id=self.test_project_id, relation__relation_name='model_of',
            class_instance_b_id=2389,
        ).class_instance_a_id

        response = self.set_targets(skeleton_ids=[skeleton_id])

        self.assertStatus(response, 409)
        self.assertEqual(response.json()['meta'], {
            'code': 'task_skeleton_already_associated', 'task_public_id': owner_id,
        })
        self.assertEqual(self.targets(), {2365})
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            project_id=self.test_project_id, relation__relation_name='annotated_with',
            class_instance_a_id=2389, class_instance_b__name=owner,
        ).exists())

    def test_task_conflict_is_scoped_to_other_tasks_in_same_study(self):
        self.name = 'neuroglass:study:00000000-0000-0000-0000-000000000001:task:00000000-0000-0000-0000-000000000003'
        other_study = 'neuroglass:study:00000000-0000-0000-0000-000000000004:task:00000000-0000-0000-0000-000000000002'
        self.assertStatus(self.set_targets(annotation=other_study, entity_ids=[2365]))
        self.assertStatus(self.set_targets(entity_ids=[2365]))
        self.assertStatus(self.set_targets(entity_ids=[2365]))
        self.assertEqual(self.targets(), {2365})

    def test_empty_targets_clear_the_annotation(self):
        self.label()
        response = self.set_targets()
        self.assertStatus(response)
        self.assertEqual(self.targets(), set())
        self.assertEqual(response.json()['entity_ids'], [])
        self.assertStatus(self.set_targets(csrfmiddlewaretoken='form-token'))

    def test_clear_preserves_the_annotation_and_its_meta_annotations(self):
        self.label()
        label = ClassInstance.objects.get(project_id=self.test_project_id, name=self.name)
        annotation._annotate_entities(self.test_project_id, [label.pk], {
            'meta-annotation': {'user_id': self.test_user_id},
        })
        self.assertStatus(self.set_targets())
        self.assertEqual(self.targets(), set())
        self.assertTrue(ClassInstance.objects.filter(pk=label.pk).exists())
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            class_instance_a=label, class_instance_b__name='meta-annotation',
        ).exists())

    def test_annotation_rename_routes_reject_existing_destination(self):
        self.label()
        other = self.create_annotation('another-annotation')
        project = self.test_project_id
        for path, data in (
            (f'neurons/{other.pk}/rename', {'name': self.name}),
            ('neurons/rename', {'names[0][0]': other.pk, 'names[0][1]': self.name}),
            (f'classification/{project}/instance-operation',
             {'operation': 'rename_node', 'id': other.pk, 'title': self.name}),
        ):
            with self.subTest(path=path), transaction.atomic():
                response = self.client.post(f'/{project}/{path}', data)
                self.assertStatus(response, 409)
                other.refresh_from_db()
                self.assertEqual(other.name, 'another-annotation')
                self.assertStatus(self.set_targets(entity_ids=[2365]))

    def test_bulk_annotation_rename_can_swap_names(self):
        first = self.create_annotation('label-one')
        second = self.create_annotation('label-two')
        response = self.client.post(f'/{self.test_project_id}/neurons/rename', {
            'names[0][0]': first.pk, 'names[0][1]': second.name,
            'names[1][0]': second.pk, 'names[1][1]': first.name,
        })
        self.assertStatus(response)
        first.refresh_from_db()
        second.refresh_from_db()
        self.assertEqual((first.name, second.name), ('label-two', 'label-one'))

    def test_unlocked_targets_use_one_query_for_edit_locks(self):
        with CaptureQueriesContext(connection) as queries:
            self.assertStatus(self.set_targets(entity_ids=[2365, 2389]))
        lock_queries = [
            query['sql'] for query in queries
            if "'locked'" in query['sql'] and 'class_instance_class_instance' in query['sql']
        ]
        self.assertEqual(len(lock_queries), 1)

    def test_bracketed_target_lists_are_supported(self):
        for fields in (
            {'entity_ids[0]': 2365, 'entity_ids[3]': 2389},
            {'entity_ids[]': '2365,2389'},
        ):
            with self.subTest(fields=fields):
                self.assertStatus(self.set_targets(**fields))
                self.assertEqual(self.targets(), {2365, 2389})

    def test_malformed_target_lists_leave_the_existing_targets_intact(self):
        self.label()
        for fields in (
            {'skeleton_ids[-1]': 123},
            {'entity_ids[0][0]': 2389},
            {'entity_ids': [2389], 'entity_ids[0]': 2365},
            {'skeleton_id': 2364},
        ):
            with self.subTest(fields=fields):
                response = self.set_targets(**fields)
                self.assertStatus(response, 400)
                self.assertEqual(self.targets(), {2365})

    def test_duplicate_annotation_fields_leave_both_target_sets_intact(self):
        self.label()
        other_name = 'other-annotation-targets-test'
        self.assertStatus(self.set_targets(annotation=other_name, entity_ids=[2389]))

        response = self.set_targets(annotation=[self.name, other_name])

        self.assertStatus(response, 400)
        self.assertEqual(self.targets(), {2365})
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            project_id=self.test_project_id, relation__relation_name='annotated_with',
            class_instance_a_id=2389, class_instance_b__name=other_name,
        ).exists())

    def test_missing_project_returns_not_found(self):
        missing_project_id = max(Project.objects.values_list('id', flat=True)) + 1
        response = self.client.post(f'/{missing_project_id}/annotations/set-targets', {
            'annotation': self.name,
        })
        self.assertStatus(response, 404)

    def test_repeated_request_preserves_the_original_annotator(self):
        owner = get_user_model().objects.create_user(username='annotation-original-owner')
        self.label(user_id=owner.pk)
        link = ClassInstanceClassInstance.objects.get(
            class_instance_a_id=2365, class_instance_b__name=self.name,
        )
        response = self.set_targets(entity_ids=[2365, 2365])
        self.assertStatus(response)
        self.assertEqual(self.targets(), {2365})
        link.refresh_from_db()
        self.assertEqual(link.user_id, owner.pk)

    def test_permission_denied_removal_rolls_back_the_added_target(self):
        owner = get_user_model().objects.create_user(username='annotation-unrelated-owner')
        self.label(user_id=owner.pk)
        response = self.set_targets(entity_ids=[2389])
        self.assertStatus(response, 403)
        self.assertEqual(self.targets(), {2365})

    def test_locked_neuron_cannot_be_selected(self):
        owner = get_user_model().objects.create_user(username='annotation-lock-owner')
        annotation._annotate_entities(self.test_project_id, [2389], {
            'locked': {'user_id': owner.pk},
        })
        self.label()
        response = self.set_targets(entity_ids=[2389])
        self.assertStatus(response, 403)
        self.assertEqual(self.targets(), {2365})

    def test_foreign_or_missing_targets_leave_the_old_link_intact(self):
        project = Project.objects.create(title='Other annotation project')
        neuron_class = Class.objects.create(
            project=project, user_id=self.test_user_id, class_name='neuron',
        )
        foreign = ClassInstance.objects.create(
            project=project, user_id=self.test_user_id,
            class_column=neuron_class, name='Other project neuron',
        )
        self.label()
        for target in [foreign.pk, 999999999]:
            with self.subTest(target=target):
                response = self.set_targets(entity_ids=[target])
                self.assertStatus(response, 404)
                self.assertEqual(self.targets(), {2365})

    def test_templates_and_reserved_annotations_are_rejected(self):
        for name, status in [('counted-{n}', 400), ('counted-{n10}', 400), ('source-fragment:protected', 403)]:
            with self.subTest(name=name):
                response = self.set_targets(annotation=name, entity_ids=[2365])
                self.assertStatus(response, status)
        self.assertEqual(self.targets(), set())

    def test_selection_does_not_silently_include_other_skeletons_on_the_same_neuron(self):
        model = ClassInstanceClassInstance.objects.get(
            project_id=self.test_project_id, relation__relation_name='model_of',
            class_instance_b_id=2389,
        )
        extra = ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id,
            class_column=model.class_instance_a.class_column, name='Second skeleton',
        )
        ClassInstanceClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id,
            relation=model.relation, class_instance_a=extra, class_instance_b_id=2389,
        )
        self.label()
        response = self.set_targets(skeleton_ids=[model.class_instance_a_id])
        self.assertStatus(response, 409)
        self.assertEqual(self.targets(), {2365})

    def test_ambiguous_annotation_name_cannot_clear_only_some_of_its_targets(self):
        self.label()
        duplicate = ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id, name=self.name,
            class_column=Class.objects.get(project_id=self.test_project_id, class_name='annotation'),
        )
        ClassInstanceClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id,
            relation=Relation.objects.get(project_id=self.test_project_id, relation_name='annotated_with'),
            class_instance_a_id=2389, class_instance_b=duplicate,
        )
        response = self.set_targets()
        self.assertStatus(response, 400)
        self.assertEqual(self.targets(), {2365, 2389})

    def test_entity_targets_can_include_annotations(self):
        target = ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id,
            class_column=Class.objects.get(
                project_id=self.test_project_id, class_name='annotation',
            ), name='Meta annotation target',
        )
        ClassInstanceClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id,
            relation=Relation.objects.get(project_id=self.test_project_id, relation_name='model_of'),
            class_instance_a_id=2364, class_instance_b=target,
        )
        response = self.set_targets(entity_ids=[target.pk])
        self.assertStatus(response)
        self.assertEqual(self.targets(), {target.pk})
        self.assertEqual(response.json()['skeleton_ids'], [])


@override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=100)
class AnnotationTargetsConcurrencyTests(AnnotationTargetsMixin, CatmaidApiTransactionTestCase):
    def wait_for_lock(self, backend_id):
        deadline = monotonic() + 5
        while monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_stat_clear_snapshot()")
                cursor.execute(
                    "SELECT wait_event_type, query FROM pg_stat_activity WHERE pid = %s",
                    [backend_id],
                )
                wait_event, query = cursor.fetchone()
                if wait_event == 'Lock':
                    return query
            sleep(.01)
        self.fail('The request did not wait for the row lock')

    @contextmanager
    def paused_replacement(self):
        ready, release = Event(), Event()
        responses, failures = [], []
        original = annotation._add_annotation_links

        def pause_add(project_id, entity_ids, annotation_map, *args, **kwargs):
            result = original(project_id, entity_ids, annotation_map, *args, **kwargs)
            if self.name in annotation_map and 2365 in entity_ids:
                ready.set()
                if not release.wait(10):
                    raise AssertionError('First replacement did not finish')
            return result

        def first_request():
            close_old_connections()
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))
                responses.append(client.post(
                    f'/{self.test_project_id}/annotations/set-targets',
                    {'annotation': self.name, 'entity_ids': [2365]},
                ))
            except BaseException as error:
                failures.append(error)
            finally:
                ready.set()
                connections['default'].close()

        with patch.object(annotation, '_add_annotation_links', side_effect=pause_add):
            worker = Thread(target=first_request, daemon=True)
            worker.start()
            try:
                self.assertTrue(ready.wait(10))
                if failures:
                    raise failures[0]
                yield
            finally:
                release.set()
                worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertStatus(responses[0])

    def test_replacements_serialize_when_the_annotation_does_not_exist_yet(self):
        with self.paused_replacement():
            response = self.client.post(
                f'/{self.test_project_id:04d}/annotations/set-targets',
                {'annotation': self.name, 'entity_ids': [2389]},
            )
            self.assertStatus(response, 409)
            self.assertEqual(response.json()['meta']['code'], 'edit_lock_timeout')
        self.assertStatus(self.set_targets(entity_ids=[2389]))
        self.assertEqual(self.targets(), {2389})
        self.assertEqual(ClassInstance.objects.filter(
            project_id=self.test_project_id, name=self.name,
            class_column__class_name='annotation',
        ).count(), 1)

    def test_independent_annotations_on_other_neurons_do_not_wait(self):
        with self.paused_replacement():
            response = self.set_targets(annotation=self.name + '-other', entity_ids=[2389])
            self.assertStatus(response)
        self.assertEqual(self.targets(), {2365})
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            class_instance_a_id=2389, class_instance_b__name=self.name + '-other',
        ).exists())

    def test_native_add_cannot_create_a_duplicate_during_first_replacement(self):
        data = {'annotations': [self.name], 'entity_ids': [2389]}
        with self.paused_replacement():
            response = self.client.post(f'/{self.test_project_id}/annotations/add', data)
            self.assertStatus(response, 409)
            self.assertEqual(response.json()['meta']['code'], 'edit_lock_timeout')
        self.assertStatus(self.client.post(f'/{self.test_project_id}/annotations/add', data))
        self.assertEqual(self.targets(), {2365, 2389})
        self.assertEqual(ClassInstance.objects.filter(
            project_id=self.test_project_id, name=self.name,
            class_column__class_name='annotation',
        ).count(), 1)

    def test_rename_respects_the_destination_name_lock(self):
        other = self.create_annotation('rename-source')
        path = f'/{self.test_project_id}/neurons/{other.pk}/rename'
        with self.paused_replacement():
            response = self.client.post(path, {'name': self.name})
            self.assertStatus(response, 409)
            self.assertEqual(response.json()['meta']['code'], 'edit_lock_timeout')
        self.assertStatus(self.client.post(path, {'name': self.name}), 409)
        self.assertStatus(self.client.post(path, {'name': 'available-name'}))
        other.refresh_from_db()
        self.assertEqual(other.name, 'available-name')

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_competing_task_annotations_serialize_on_the_same_neuron(self):
        prefix = 'neuroglass:study:00000000-0000-0000-0000-000000000001:task:'
        task_ids = ['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003']
        ready_to_lock = Barrier(2)

        def associate(task_id):
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))

                def wait_for_other_request(execute, sql, params, many, context):
                    if ('FOR UPDATE' in sql and 'ORDER BY' in sql
                            and '"class_instance"."id" IN' in sql):
                        ready_to_lock.wait(timeout=8)
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(wait_for_other_request):
                    return client.post(f'/{self.test_project_id}/annotations/set-targets', {
                        'annotation': prefix + task_id, 'entity_ids': [2365],
                    })
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=2) as workers:
            responses = list(workers.map(associate, task_ids))
        self.assertCountEqual([200, 409], [response.status_code for response in responses])
        winner, = [task_id for task_id, response in zip(task_ids, responses) if response.status_code == 200]
        rejected, = [response for response in responses if response.status_code == 409]
        self.assertEqual(rejected.json()['meta'], {
            'code': 'task_skeleton_already_associated', 'task_public_id': winner,
        })
        labels = list(ClassInstanceClassInstance.objects.filter(
            project_id=self.test_project_id, relation__relation_name='annotated_with',
            class_instance_a_id=2365, class_instance_b__name__startswith=prefix,
        ).values_list('class_instance_b__name', flat=True))
        self.assertEqual(labels, [prefix + winner])

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_crossed_meta_annotation_replacements_do_not_deadlock(self):
        first = self.create_annotation(self.name)
        second = self.create_annotation('another-annotation')
        ready_to_lock = Barrier(2)

        def replace(source, target):
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))

                def wait_for_other_request(execute, sql, params, many, context):
                    if ('FOR UPDATE' in sql and 'ORDER BY' in sql
                            and '"class_instance"."id" IN' in sql):
                        ready_to_lock.wait(timeout=8)
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(wait_for_other_request):
                    return client.post(f'/{self.test_project_id}/annotations/set-targets', {
                        'annotation': source.name, 'entity_ids': [target.pk],
                    })
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=2) as workers:
            forward = workers.submit(replace, first, second)
            reverse = workers.submit(replace, second, first)
            self.assertStatus(forward.result(timeout=15))
            self.assertStatus(reverse.result(timeout=15))
        self.assertEqual(self.targets(), {second.pk})
        self.assertTrue(ClassInstanceClassInstance.objects.filter(
            class_instance_a=first, class_instance_b=second,
        ).exists())

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=15000)
    def test_replacement_returns_conflict_if_an_existing_link_is_locked(self):
        self.label()
        started = Event()

        def replace():
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))
                started.set()
                return client.post(f'/{self.test_project_id}/annotations/set-targets', {
                    'annotation': self.name, 'entity_ids': [2389],
                })
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=1) as workers:
            with transaction.atomic():
                ClassInstanceClassInstance.objects.select_for_update(of=('self',)).get(
                    class_instance_a_id=2365, class_instance_b__name=self.name,
                )
                pending = workers.submit(replace)
                self.assertTrue(started.wait(10))
                response = pending.result(timeout=5)
                self.assertStatus(response, 409)
                self.assertEqual(response.json()['meta']['code'], 'edit_lock_timeout')
        self.assertEqual(self.targets(), {2365})

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_bulk_neuron_rename_and_replacement_lock_rows_in_the_same_order(self):
        self.assert_bulk_rename_and_replacement_succeed('neuron')

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_mixed_bulk_rename_and_replacement_lock_rows_in_the_same_order(self):
        self.assert_bulk_rename_and_replacement_succeed('annotation')

    def assert_bulk_rename_and_replacement_succeed(self, class_name):
        neuron = ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id, name='neuron-to-rename',
            class_column=Class.objects.get(project_id=self.test_project_id, class_name='neuron'),
        )
        other = ClassInstance.objects.create(
            project_id=self.test_project_id, user_id=self.test_user_id, name='other-to-rename',
            class_column=Class.objects.get(project_id=self.test_project_id, class_name=class_name),
        )
        started = Event()
        backend_ids = []

        def rename():
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))
                with connection.cursor() as cursor:
                    cursor.execute('SELECT pg_backend_pid()')
                    backend_ids.append(cursor.fetchone()[0])
                started.set()
                return client.post(f'/{self.test_project_id}/neurons/rename', {
                    'names[0][0]': other.pk, 'names[0][1]': f'renamed-{class_name}-target',
                    'names[1][0]': neuron.pk, 'names[1][1]': 'renamed-neuron',
                })
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=1) as workers:
            with transaction.atomic():
                ClassInstance.objects.select_for_update().get(pk=neuron.pk)
                renaming = workers.submit(rename)
                self.assertTrue(started.wait(10))
                waiting_query = self.wait_for_lock(backend_ids[0])
                self.assertIn('FOR UPDATE', waiting_query)
                self.assertStatus(self.set_targets(entity_ids=[neuron.pk, other.pk]))
            self.assertStatus(renaming.result(timeout=15))
        self.assertEqual(self.targets(), {neuron.pk, other.pk})

    def test_native_remove_returns_conflict_during_replacement(self):
        self.label(target=2389)
        label = ClassInstance.objects.get(
            project_id=self.test_project_id, name=self.name,
            class_column__class_name='annotation',
        )
        with self.paused_replacement(), transaction.atomic():
            # Bound a native DELETE wait so a lock-order regression fails quickly.
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '100ms'")
            response = self.client.post(
                f'/{self.test_project_id}/annotations/remove',
                {'annotation_ids': [label.pk], 'entity_ids': [2389]},
            )
            self.assertStatus(response, 409)
            self.assertEqual(response.json()['meta']['code'], 'edit_lock_timeout')
        self.assertEqual(self.targets(), {2365})

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_skeleton_mapping_is_rechecked_after_waiting_for_the_neuron(self):
        model = ClassInstanceClassInstance.objects.get(
            project_id=self.test_project_id, relation__relation_name='model_of',
            class_instance_b_id=2365,
        )
        started = Event()
        backend_ids, responses, failures = [], [], []

        def replace():
            close_old_connections()
            try:
                client = Client()
                client.force_login(get_user_model().objects.get(pk=self.test_user_id))
                with connections['default'].cursor() as cursor:
                    cursor.execute('SELECT pg_backend_pid()')
                    backend_ids.append(cursor.fetchone()[0])
                started.set()
                responses.append(client.post(
                    f'/{self.test_project_id}/annotations/set-targets',
                    {'annotation': self.name, 'skeleton_ids': [model.class_instance_a_id]},
                ))
            except BaseException as error:
                failures.append(error)
            finally:
                connections['default'].close()

        with transaction.atomic():
            ClassInstance.objects.select_for_update().get(pk=2365)
            worker = Thread(target=replace, daemon=True)
            worker.start()
            self.addCleanup(worker.join, 10)
            self.assertTrue(started.wait(10))
            self.wait_for_lock(backend_ids[0])
            ClassInstanceClassInstance.objects.filter(pk=model.pk).update(
                class_instance_b_id=2389,
            )
        worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertStatus(responses[0], 409)
        self.assertEqual(self.targets(), set())
