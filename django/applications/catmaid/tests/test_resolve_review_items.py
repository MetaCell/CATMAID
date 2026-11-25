import gzip
import json
import tempfile
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from catmaid.models import ClassInstance, Treenode, TreenodeClassInstance, User
from catmaid.tests.common import init_consistent_data

LABEL = "neuroglass:review-item"
PROJECT = 3


def point(role, x, y, z, anchor=None):
    return {"role": role, "x": x, "y": y, "z": z, "anchor": anchor}


def row(line, kind, *points):
    return {"line": line, "kind": kind, "confidence": 0.9, "priority": None, "description": "",
            "points": list(points)}


class ResolveReviewItemsCommandTests(TestCase):
    """NGLASS-2386 (planning page, phase 2 model): review items arrive as positions in nm, and the
    import finds the treenode at each position inside the study's project, within a 5 nm margin on
    every axis (CATMAID stores locations as 32-bit floats), then labels the found nodes in the same
    transaction. Rows that cannot be placed are skipped with a reason; when none can, the step fails
    and labels nothing. Fixture project 3: skeleton 1 is the chain 7 (root, 3590,3240,0) → 11
    (3600,3250,9) → 13 (3600,3250,18) → 15 (3590,3250,27)."""
    fixtures = ["catmaid_testdata"]

    def setUp(self):
        init_consistent_data()
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.user = User.objects.create(username="review-item-resolver", is_superuser=True)

    def resolve(self, rows, project_id=PROJECT):
        points_path = self.directory / "points.jsonl.gz"
        resolved_path, result_path = self.directory / "resolved.jsonl.gz", self.directory / "resolve-result.json"
        with gzip.open(points_path, "wt") as handle:
            handle.writelines(json.dumps(each) + "\n" for each in rows)
        try:
            call_command("resolve_review_items", "--project-id", str(project_id), "--label", LABEL,
                         "--points", str(points_path), "--resolved", str(resolved_path),
                         "--result", str(result_path), "--user-id", str(self.user.pk))
        finally:
            self.result = json.loads(result_path.read_text()) if result_path.exists() else None
        with gzip.open(resolved_path, "rt") as handle:
            self.resolved = [json.loads(line) for line in handle]
        return self.result

    def anchors(self):
        return [[each["anchor_id"] for each in record["points"]] for record in self.resolved]

    def skipped(self):
        return [(each["line"], each["reason"]) for each in self.result["skipped"]]

    def labelled_ids(self):
        return set(TreenodeClassInstance.objects.filter(
            project_id=PROJECT, relation__relation_name="labeled_as",
            class_instance__in=ClassInstance.objects.filter(project_id=PROJECT, name=LABEL),
        ).values_list("treenode_id", flat=True))

    def test_position_resolves_to_its_node(self):
        result = self.resolve([row(2, "merge_error", point("a", 3600.0, 3250.0, 9.0))])
        self.assertEqual([[11]], self.anchors(), "The point sits on treenode 11, which becomes its anchor")
        self.assertEqual(("succeeded", 1, 0), (result["outcome"], result["resolved"], result["skipped_count"]),
                         "One row placed, none skipped")
        self.assertEqual({11}, self.labelled_ids(), "The anchor is protected before any item points to it")

    def test_float32_storage_still_matches(self):
        Treenode.objects.filter(pk=15).update(location_x=31453002)
        self.resolve([row(2, "merge_error", point("a", 31453000.5, 3250.0, 27.0))])
        self.assertEqual([[15]], self.anchors(),
                         "A float32 location is off by up to a few nm at this scale; the 5 nm margin absorbs it")

    def test_two_nodes_within_margin_are_ambiguous(self):
        self.resolve([row(2, "merge_error", point("a", 3600.0, 3250.0, 13.5)),
                       row(3, "merge_error", point("a", 3600.0, 3250.0, 9.0))])
        self.assertEqual([(2, "ambiguous_position")], self.skipped(),
                         "Nodes 11 and 13 are both 4.5 nm away; picking one would be a guess")
        self.assertEqual([[11]], self.anchors(), "The other row still resolves")

    def test_given_anchor_must_match(self):
        self.resolve([row(2, "merge_error", point("a", 3600.0, 3250.0, 9.0, anchor=13)),
                      row(3, "merge_error", point("a", 3600.0, 3250.0, 18.0, anchor=13))])
        self.assertEqual([(2, "anchor_mismatch")], self.skipped(),
                         "The file says node 13 but the position holds node 11: the row is not trusted")
        self.assertEqual([[13]], self.anchors(), "A matching anchor resolves")

    def test_merge_error_at_root_is_skipped(self):
        self.resolve([row(2, "merge_error", point("a", 3590.0, 3240.0, 0.0)),
                       row(3, "merge_error", point("a", 3600.0, 3250.0, 9.0))])
        self.assertEqual([(2, "merge_node_is_root")], self.skipped(),
                         "A merge error is fixed by cutting above its node; a root has nothing above it")

    def test_split_error_on_one_skeleton_is_skipped(self):
        self.resolve([row(2, "split_error", point("a", 3600.0, 3250.0, 9.0), point("b", 3600.0, 3250.0, 18.0)),
                      row(3, "split_error", point("a", 3600.0, 3250.0, 9.0), point("b", 1065.0, 3035.0, 0.0))])
        self.assertEqual([(2, "split_same_skeleton")], self.skipped(),
                         "Joining a skeleton to itself would make a loop")
        self.assertEqual([[11, 237]], self.anchors(), "Points on skeletons 1 and 235 can be joined")

    def test_nodes_of_another_project_are_ignored(self):
        with self.assertRaises(CommandError):
            self.resolve([row(2, "merge_error", point("a", 3600.0, 3250.0, 9.0))], project_id=1)
        self.assertEqual([(2, "no_node_at_position")], self.skipped(),
                         "Node 11 is in project 3; the study's project 1 has no node there")

    def test_resolved_nodes_are_labelled_once(self):
        rows = [row(2, "split_error", point("a", 3600.0, 3250.0, 9.0), point("b", 1065.0, 3035.0, 0.0))]
        self.resolve(rows)
        result = self.resolve(rows)
        self.assertEqual((0, 2), (result["labelled"], result["already"]), "A re-run adds no second label")
        self.assertEqual({11, 237}, self.labelled_ids(), "Both points' nodes carry the label")

    def test_nothing_resolved_fails_and_labels_nothing(self):
        with self.assertRaises(CommandError):
            self.resolve([row(2, "merge_error", point("a", 3590.0, 3240.0, 0.0)),
                          row(3, "merge_error", point("a", 9.0, 9.0, 9.0))])
        self.assertEqual(("failed", "review_items_unresolved", 2),
                         (self.result["outcome"], self.result["category"], self.result["skipped_count"]),
                         "An import that places nothing is a failure, not an empty success")
        self.assertEqual([(2, "merge_node_is_root"), (3, "no_node_at_position")], self.skipped(),
                         "Each row says why it was skipped")
        self.assertEqual(set(), self.labelled_ids(), "Nothing is protected for items that will not exist")
