"""Remove a label from every treenode of a project except listed ones (NGLASS-2386).

NeuroGlass uses it to drop review-item labels left by an abandoned import: the keep list holds
every treenode a review item still points to.
"""
import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction

from catmaid.models import ClassInstance, TreenodeClassInstance


class Command(BaseCommand):
    help = "Remove LABEL from every treenode of the project that is not in the keep file (one ID per line)."

    def add_arguments(self, parser):
        parser.add_argument("--project-id", type=int, required=True)
        parser.add_argument("--label", required=True)
        parser.add_argument("--keep-treenode-ids", type=Path, required=True)
        parser.add_argument("--result", type=Path, required=True)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        project_id, name, dry_run = options["project_id"], options["label"], options["dry_run"]
        keep = {int(line) for line in options["keep_treenode_ids"].read_text().split()}
        with transaction.atomic():
            links = TreenodeClassInstance.objects.filter(
                project_id=project_id, relation__relation_name="labeled_as", class_instance__name=name,
                class_instance__class_column__class_name="label",
            )
            labelled = list(links.values_list("pk", "treenode_id"))
            orphan_links = [pk for pk, treenode_id in labelled if treenode_id not in keep]
            if not dry_run and orphan_links:
                TreenodeClassInstance.objects.filter(pk__in=orphan_links).delete()
                ClassInstance.objects.filter(project_id=project_id, name=name, class_column__class_name="label",
                                             treenodeclassinstance__isnull=True,
                                             connectorclassinstance__isnull=True).delete()
        result = {"outcome": "succeeded", "removed": len(orphan_links), "kept": len(labelled) - len(orphan_links),
                  "dry_run": dry_run}
        options["result"].write_text(json.dumps(result))
        self.stdout.write(json.dumps(result))
