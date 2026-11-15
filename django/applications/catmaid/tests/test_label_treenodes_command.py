import json
import tempfile
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from catmaid.models import ClassInstance, TreenodeClassInstance, User
from catmaid.tests.common import init_consistent_data

LABEL = "neuroglass:review-item"


class LabelTreenodesCommandTests(TestCase):
    """NGLASS-2386: review-item nodes are labelled in bulk at import, idempotently, and only
    when every node still exists in the project."""
    fixtures = ["catmaid_testdata"]

    def setUp(self):
        init_consistent_data()
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.user = User.objects.create(username="label-importer", is_superuser=True)

    def run_command(self, ids):
        ids_path, result_path = self.directory / "ids.txt", self.directory / "result.json"
        ids_path.write_text("".join(f"{treenode_id}\n" for treenode_id in ids))
        try:
            call_command("label_treenodes", "--project-id", "3", "--label", LABEL, "--treenode-ids", str(ids_path),
                         "--result", str(result_path), "--user-id", str(self.user.pk))
        finally:
            self.result = json.loads(result_path.read_text()) if result_path.exists() else None
        return self.result

    def labelled_ids(self):
        return set(TreenodeClassInstance.objects.filter(
            project_id=3, relation__relation_name="labeled_as",
            class_instance__in=ClassInstance.objects.filter(project_id=3, name=LABEL),
        ).values_list("treenode_id", flat=True))

    def test_labels_nodes_once(self):
        self.run_command([7, 11])
        result = self.run_command([7, 11, 13])
        self.assertEqual((1, 2), (result["labelled"], result["already"]), "A re-run only adds what is missing")
        self.assertEqual({7, 11, 13}, self.labelled_ids(), "All three carry the label")

    def test_a_missing_node_labels_nothing(self):
        with self.assertRaises(CommandError):
            self.run_command([7, 999999])
        self.assertEqual(set(), self.labelled_ids(), "Nothing is labelled when any node is gone")
        self.assertEqual(("treenodes_missing", [999999]), (self.result["category"], self.result["missing"]),
                         "The import's reason names the missing node")
