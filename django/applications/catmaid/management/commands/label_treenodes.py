"""Label many treenodes of one project at once, for NeuroGlass review-item imports (NGLASS-2386)."""
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from catmaid.models import Class, ClassInstance, Relation, Treenode, TreenodeClassInstance, User

CHUNK = 10_000
REPORTED_MISSING = 100


def _chunks(values):
    for start in range(0, len(values), CHUNK):
        yield values[start:start + CHUNK]


class Command(BaseCommand):
    help = ("Label every treenode listed in a file (one ID per line) in one project. Nothing is labelled "
            "when any listed treenode is not in the project; nodes already carrying the label are skipped.")

    def add_arguments(self, parser):
        parser.add_argument("--project-id", type=int, required=True)
        parser.add_argument("--label", required=True)
        parser.add_argument("--treenode-ids", type=Path, required=True)
        parser.add_argument("--result", type=Path, required=True)
        parser.add_argument("--user-id", type=int, help="Owner of new label links; defaults to the first superuser")

    def handle(self, *args, **options):
        project_id, label_name, result_path = options["project_id"], options["label"], options["result"]
        ids = sorted({int(line) for line in options["treenode_ids"].read_text().split()})
        user = (User.objects.get(pk=options["user_id"]) if options["user_id"]
                else User.objects.filter(is_superuser=True).order_by("pk").first())
        if user is None:
            raise CommandError("No user to own the labels; pass --user-id")
        with transaction.atomic():
            present = set()
            for chunk in _chunks(ids):
                present.update(Treenode.objects.filter(project_id=project_id, id__in=chunk).values_list("id", flat=True))
            missing = [treenode_id for treenode_id in ids if treenode_id not in present]
            if missing:
                result_path.write_text(json.dumps({
                    "outcome": "failed", "category": "treenodes_missing", "missing": missing[:REPORTED_MISSING],
                    "message": f"{len(missing)} treenodes are not in project {project_id}",
                }))
                raise CommandError(f"{len(missing)} treenodes are not in project {project_id}; nothing was labelled")
            label = self._label(project_id, label_name, user)
            relation = Relation.objects.get(project_id=project_id, relation_name="labeled_as")
            already = set()
            for chunk in _chunks(ids):
                already.update(TreenodeClassInstance.objects.filter(
                    project_id=project_id, relation=relation, class_instance=label, treenode_id__in=chunk,
                ).values_list("treenode_id", flat=True))
            TreenodeClassInstance.objects.bulk_create(
                (TreenodeClassInstance(user=user, project_id=project_id, relation=relation, treenode_id=treenode_id,
                                       class_instance=label)
                 for treenode_id in ids if treenode_id not in already),
                batch_size=CHUNK,
            )
        result = {"outcome": "succeeded", "labelled": len(ids) - len(already), "already": len(already)}
        result_path.write_text(json.dumps(result))
        self.stdout.write(json.dumps(result))

    def _label(self, project_id, name, user):
        label_class = Class.objects.get(project_id=project_id, class_name="label")
        label = ClassInstance.objects.filter(project_id=project_id, class_column=label_class, name=name).order_by("pk").first()
        return label or ClassInstance.objects.create(project_id=project_id, class_column=label_class, name=name, user=user)
