"""Shared set-up for NGLASS-2386 review-item tests: label nodes the way imports do."""
import tempfile
from pathlib import Path

from django.core.management import call_command

from catmaid.models import ClassInstance, Relation, TreenodeClassInstance, User

REVIEW_ITEM_LABEL = "neuroglass:review-item"


def _importer():
    return User.objects.filter(is_superuser=True).order_by("pk").first() or User.objects.create(
        username="review-item-importer", is_superuser=True)


def label_nodes(project_id, treenode_ids, label=REVIEW_ITEM_LABEL):
    with tempfile.TemporaryDirectory() as directory:
        ids_path = Path(directory) / "ids.txt"
        ids_path.write_text("".join(f"{treenode_id}\n" for treenode_id in treenode_ids))
        call_command("label_treenodes", "--project-id", str(project_id), "--label", label,
                     "--treenode-ids", str(ids_path), "--result", str(Path(directory) / "result.json"),
                     "--user-id", str(_importer().pk))
