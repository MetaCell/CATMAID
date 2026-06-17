"""Create a hidden CATMAID project and metadata stack from an import config.

Run this through ``manage.py shell`` inside the CATMAID app container. The
project is intentionally left hidden from normal users. Publish it later with
``publish_project_permissions.sh`` after import, materialization, cache rebuild,
and verification complete.
"""

import json
import os

from catmaid.apps import get_system_user
from catmaid.control.project import validate_project_setup
from catmaid.control.tracing import setup_tracing
from catmaid.models import Project, ProjectStack, Stack
from guardian.shortcuts import assign_perm, get_groups_with_perms, get_users_with_perms, remove_perm


def load_config():
    inline_config = os.environ.get("IMPORT_CONFIG_JSON")
    if inline_config:
        return json.loads(inline_config)

    path = os.environ.get("IMPORT_CONFIG")
    if not path:
        raise ValueError("IMPORT_CONFIG or IMPORT_CONFIG_JSON is required")
    with open(path) as f:
        return json.load(f)


def config_get(config, *keys):
    value = config
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def parse_3tuple(value, cast, name):
    if value is None:
        raise ValueError("%s is required" % name)
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    else:
        parts = list(value)
    if len(parts) != 3:
        raise ValueError("%s must have three values" % name)
    return tuple(cast(part) for part in parts)


def as_tuple(value):
    if all(hasattr(value, attr) for attr in ("x", "y", "z")):
        return (value.x, value.y, value.z)
    return tuple(value)


def build_neuroglancer_metadata(config):
    raw_metadata = config_get(config, "stack", "metadata") or {}
    if not isinstance(raw_metadata, dict):
        raise ValueError("stack.metadata must be an object")

    allowed_metadata_keys = {"cache_provider", "read_only", "spatial"}
    unknown_keys = set(raw_metadata) - allowed_metadata_keys
    if unknown_keys:
        raise ValueError(
            "stack.metadata contains unsupported Neuroglancer keys: %s"
            % ", ".join(sorted(unknown_keys))
        )

    metadata = {}

    if "cache_provider" in raw_metadata:
        cache_provider = raw_metadata["cache_provider"]
        if not isinstance(cache_provider, str):
            raise ValueError("stack.metadata.cache_provider must be a string")
        metadata["cache_provider"] = cache_provider

    if "read_only" in raw_metadata:
        read_only = raw_metadata["read_only"]
        if not isinstance(read_only, bool):
            raise ValueError("stack.metadata.read_only must be a boolean")
        metadata["read_only"] = read_only

    if "spatial" in raw_metadata:
        spatial = raw_metadata["spatial"]
        if not isinstance(spatial, list):
            raise ValueError("stack.metadata.spatial must be an array")
        spatial_levels = []
        for index, level in enumerate(spatial):
            if not isinstance(level, dict):
                raise ValueError("stack.metadata.spatial[%s] must be an object" % index)
            unknown_level_keys = set(level) - {"chunk_size", "limit"}
            if unknown_level_keys:
                raise ValueError(
                    "stack.metadata.spatial[%s] contains unsupported keys: %s"
                    % (index, ", ".join(sorted(unknown_level_keys)))
                )
            if "chunk_size" not in level or "limit" not in level:
                raise ValueError("stack.metadata.spatial[%s] requires chunk_size and limit" % index)
            spatial_levels.append(
                {
                    "chunk_size": list(parse_3tuple(level["chunk_size"], int, "stack.metadata.spatial[%s].chunk_size" % index)),
                    "limit": int(level["limit"]),
                }
            )
        if spatial_levels:
            metadata["spatial"] = spatial_levels

    return metadata


def clear_project_permissions(project):
    for user, perms in get_users_with_perms(
        project,
        attach_perms=True,
        with_group_users=False,
        with_superusers=False,
    ).items():
        for perm in perms:
            remove_perm(perm, user, project)

    for group, perms in get_groups_with_perms(project, attach_perms=True).items():
        for perm in perms:
            remove_perm(perm, group, project)


config = load_config()
project_title = os.environ.get("PROJECT_TITLE") or config_get(config, "project", "title") or config["dataset_id"]
stack_title = os.environ.get("STACK_TITLE") or config_get(config, "stack", "title") or project_title
dimension = parse_3tuple(
    os.environ.get("STACK_DIMENSION") or config_get(config, "stack", "dimension"),
    int,
    "stack.dimension",
)
resolution = parse_3tuple(
    os.environ.get("STACK_RESOLUTION") or config_get(config, "stack", "resolution_nm"),
    float,
    "stack.resolution_nm",
)
translation = parse_3tuple(
    os.environ.get("STACK_TRANSLATION") or config_get(config, "stack", "translation") or (0, 0, 0),
    float,
    "stack.translation",
)
orientation = int(os.environ.get("STACK_ORIENTATION") or config_get(config, "stack", "orientation") or 0)
canary_location = parse_3tuple(
    os.environ.get("STACK_CANARY_LOCATION")
    or config_get(config, "stack", "canary_location")
    or (dimension[0] // 2, dimension[1] // 2, dimension[2] // 2),
    int,
    "stack.canary_location",
)
metadata = build_neuroglancer_metadata(config)

project, project_created = Project.objects.get_or_create(
    title=project_title,
    defaults={
        "comment": "Hidden skeleton bulk import project for %s." % config.get("dataset_id", project_title)
    },
)
system_user = get_system_user()

validate_project_setup(project.id, system_user.id, fix=True)
setup_tracing(project.id, system_user)

stack, stack_created = Stack.objects.get_or_create(
    title=stack_title,
    defaults={
        "dimension": dimension,
        "resolution": resolution,
        "comment": "Metadata-only stack for %s skeleton import." % config.get("dataset_id", project_title),
        "description": "Coordinate space for %s." % project_title,
        "downsample_factors": None,
        "metadata": metadata,
        "canary_location": canary_location,
        "placeholder_color": (0, 0, 0, 1),
    },
)

stack_metadata_updated = False
if not stack_created:
    if tuple(int(v) for v in as_tuple(stack.dimension)) != tuple(int(v) for v in dimension):
        raise ValueError("Existing stack dimension does not match import config")
    if tuple(float(v) for v in as_tuple(stack.resolution)) != tuple(float(v) for v in resolution):
        raise ValueError("Existing stack resolution does not match import config")
    if stack.metadata != metadata:
        stack.metadata = metadata
        stack.save(update_fields=["metadata"])
        stack_metadata_updated = True

project_stack, project_stack_created = ProjectStack.objects.get_or_create(
    project=project,
    stack=stack,
    defaults={"translation": translation, "orientation": orientation},
)

project_stack_updated = False
if not project_stack_created:
    updates = []
    if tuple(float(v) for v in as_tuple(project_stack.translation)) != tuple(float(v) for v in translation):
        project_stack.translation = translation
        updates.append("translation")
    if project_stack.orientation != orientation:
        project_stack.orientation = orientation
        updates.append("orientation")
    if updates:
        project_stack.save(update_fields=updates)
        project_stack_updated = True

clear_project_permissions(project)
for permission in ("can_browse", "can_annotate", "can_import", "can_administer"):
    assign_perm(permission, system_user, project)

print("PROJECT_CREATED=%s" % project_created)
print("STACK_CREATED=%s" % stack_created)
print("STACK_METADATA_UPDATED=%s" % stack_metadata_updated)
print("PROJECT_STACK_CREATED=%s" % project_stack_created)
print("PROJECT_STACK_UPDATED=%s" % project_stack_updated)
print("PROJECT_HIDDEN_FROM_TEMPLATE_USERS=true")
print("PROJECT_ID=%s" % project.id)
print("STACK_ID=%s" % stack.id)
print("IMPORT_USER_ID=%s" % system_user.id)
