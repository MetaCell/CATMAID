"""Publish a hidden bulk-import project by copying template permissions."""

import os

from catmaid.apps import get_system_user
from catmaid.models import Project
from guardian.shortcuts import assign_perm, get_groups_with_perms, get_users_with_perms, remove_perm


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


def copy_project_permissions(source_project, target_project):
    clear_project_permissions(target_project)

    for source_user, perms in get_users_with_perms(
        source_project,
        attach_perms=True,
        with_group_users=False,
        with_superusers=False,
    ).items():
        for perm in perms:
            assign_perm(perm, source_user, target_project)

    for source_group, perms in get_groups_with_perms(source_project, attach_perms=True).items():
        for perm in perms:
            assign_perm(perm, source_group, target_project)


project = Project.objects.get(pk=int(os.environ["PROJECT_ID"]))
template_project = Project.objects.get(pk=int(os.environ["TEMPLATE_PROJECT_ID"]))
if project.id == template_project.id:
    raise ValueError("Target project cannot be the template project")

copy_project_permissions(template_project, project)

system_user = get_system_user()
for permission in ("can_browse", "can_annotate", "can_import", "can_administer"):
    assign_perm(permission, system_user, project)

print("PROJECT_ID=%s" % project.id)
print("PUBLISHED_PERMISSIONS_FROM_PROJECT_ID=%s" % template_project.id)
