"""Atomic replacement of one annotation's complete target set."""
from contextlib import contextmanager
from threading import Event, Thread
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections, connections
from django.test import Client, override_settings

from catmaid.control import annotation
from catmaid.models import Class, ClassInstance, ClassInstanceClassInstance, Project
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

    def test_empty_targets_clear_the_annotation(self):
        self.label()
        response = self.set_targets()
        self.assertStatus(response)
        self.assertEqual(self.targets(), set())
        self.assertEqual(response.json()['entity_ids'], [])
        self.assertStatus(self.set_targets())

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
        from catmaid.models import Relation
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
        response = self.set_targets(entity_ids=[target.pk])
        self.assertStatus(response)
        self.assertEqual(self.targets(), {target.pk})


@override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=100)
class AnnotationTargetsConcurrencyTests(AnnotationTargetsMixin, CatmaidApiTransactionTestCase):
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

    @override_settings(SKELETON_EDIT_LOCK_TIMEOUT_MS=5000)
    def test_skeleton_mapping_is_rechecked_after_waiting_for_the_neuron(self):
        from time import monotonic, sleep
        from django.db import connection, transaction

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
            deadline = monotonic() + 5
            while monotonic() < deadline:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                        [backend_ids[0]],
                    )
                    if cursor.fetchone()[0] == 'Lock':
                        break
                sleep(.01)
            else:
                self.fail('The replacement did not wait for the neuron lock')
            ClassInstanceClassInstance.objects.filter(pk=model.pk).update(
                class_instance_b_id=2389,
            )
        worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertStatus(responses[0], 409)
        self.assertEqual(self.targets(), set())
