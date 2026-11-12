# -*- coding: utf-8 -*-

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

ONTOLOGY_DUMMY_PROJECT_ID = getattr(settings, 'ONTOLOGY_DUMMY_PROJECT_ID', None)
if ONTOLOGY_DUMMY_PROJECT_ID is None:
    raise ImproperlyConfigured("In order to use CATMAID's ontology and "
        "classification system you have to configure ONTOLOGY_DUMMY_PROJECT_ID "
        "in your settings module.")


def get_skeleton_join_annotation_policy():
    policy = getattr(settings, 'SKELETON_JOIN_ANNOTATION_POLICY', None)
    if policy is None:
        return None
    if isinstance(policy, str):
        try:
            policy = import_string(policy)
        except ImportError as error:
            raise ImproperlyConfigured(
                'Could not import SKELETON_JOIN_ANNOTATION_POLICY.'
            ) from error
    if not callable(policy):
        raise ImproperlyConfigured(
            'SKELETON_JOIN_ANNOTATION_POLICY must be a callable or a dotted path to one.'
        )
    return policy
