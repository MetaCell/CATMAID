import json
import tempfile
from pathlib import Path

from django.core.management import call_command
from django.test import TestCase

from catmaid.review_items import REVIEW_ITEM_LABEL, labelled_treenode_ids
from catmaid.tests.common import init_consistent_data
from catmaid.tests.review_item_helpers import add_plain_label, has_label, label_nodes


class PruneTreenodeLabelsCommandTests(TestCase):
    """NGLASS-2386 (spec 7.4): labels left by an abandoned import, which no review item points
    to, can be removed; nothing else is touched."""
    fixtures = ["catmaid_testdata"]

    def setUp(self):
        init_consistent_data()
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        label_nodes(3, [7, 11, 13])
        add_plain_label(3, 11, "axon")

    def run_command(self, keep, dry_run=False):
        keep_path, result_path = self.directory / "keep.txt", self.directory / "result.json"
        keep_path.write_text("".join(f"{treenode_id}\n" for treenode_id in keep))
        call_command("prune_treenode_labels", "--project-id", "3", "--label", REVIEW_ITEM_LABEL,
                     "--keep-treenode-ids", str(keep_path), "--result", str(result_path),
                     *(["--dry-run"] if dry_run else []))
        return json.loads(result_path.read_text())

    def test_unreferenced_nodes_lose_only_the_review_item_label(self):
        result = self.run_command(keep=[7])
        self.assertEqual((2, 1), (result["removed"], result["kept"]), "11 and 13 had no item")
        self.assertEqual([7], labelled_treenode_ids(3, treenode_ids=[7, 11, 13]), "Only node 7 keeps it")
        self.assertTrue(has_label(3, 11, "axon"), "Other labels on the same node stay")

    def test_dry_run_changes_nothing(self):
        self.assertEqual(2, self.run_command(keep=[7], dry_run=True)["removed"], "It reports what it would remove")
        self.assertEqual([7, 11, 13], labelled_treenode_ids(3, treenode_ids=[7, 11, 13]), "Nothing is removed")
