"""Literal, importer-owned source fragment annotations.

The reserved namespace stores source identifiers, not editable labels. Public
annotation operations must preflight their complete request before writing.
Structural operations may transfer existing provenance through private helpers.
"""

SOURCE_FRAGMENT_ANNOTATION_PREFIX = "source-fragment:"


def is_source_fragment_annotation_name(name):
    return isinstance(name, str) and name.startswith(SOURCE_FRAGMENT_ANNOTATION_PREFIX)


def reject_reserved_annotation_names(names):
    if any(is_source_fragment_annotation_name(name) for name in names):
        from catmaid.control.authentication import PermissionError
        raise PermissionError("Source fragment annotations are importer-owned and cannot be edited manually")


def reject_reserved_annotations(project_id, annotation_ids):
    from catmaid.models import ClassInstance
    # Check the actual objects, independently of the request's project. Some
    # generic legacy routes accept entity IDs before enforcing project scope.
    names = ClassInstance.objects.filter(
        id__in=annotation_ids,
        class_column__class_name="annotation",
        name__startswith=SOURCE_FRAGMENT_ANNOTATION_PREFIX,
    ).values_list("name", flat=True)
    reject_reserved_annotation_names(names)


def reject_reserved_annotation_rename(instance, new_name):
    if instance.class_column.class_name == "annotation":
        reject_reserved_annotation_names((instance.name, new_name))
