from django.test import TestCase

from catmaid.control.authentication import PermissionError
from catmaid.review_items import (
    REVIEW_ITEM_LABEL, ReviewItemNodesProtected, guard_review_item_nodes, labelled_treenode_ids,
    reject_reserved_label_change,
)
from catmaid.tests.common import init_consistent_data
from catmaid.tests.review_item_helpers import label_nodes


class ReviewItemModuleTests(TestCase):
    """NGLASS-2386 (spec 8.4): treenodes carrying review items are found per project and their
    deletion needs confirmation."""
    fixtures = ["catmaid_testdata"]

    def setUp(self):
        init_consistent_data()
        label_nodes(3, [7, 13])

    def test_labelled_nodes_are_found_by_node_or_skeleton(self):
        self.assertEqual([13], labelled_treenode_ids(3, treenode_ids=[11, 13]), "Only labelled nodes are returned")
        self.assertEqual([7, 13], labelled_treenode_ids(3, skeleton_ids=[1]), "A skeleton's labelled nodes are all found")

    def test_lookup_is_scoped_to_the_project(self):
        self.assertEqual([], labelled_treenode_ids(4, treenode_ids=[7]), "Treenode 7 is not protected in another project")

    def test_guard_raises_409_with_the_ids_unless_confirmed(self):
        with self.assertRaises(ReviewItemNodesProtected) as raised:
            guard_review_item_nodes(3, confirmed=False, skeleton_ids=[1])
        self.assertEqual((409, {"label": REVIEW_ITEM_LABEL, "count": 2, "treenode_ids": [7, 13]}),
                         (raised.exception.status_code, raised.exception.meta), "The viewer needs the count and the IDs")
        guard_review_item_nodes(3, confirmed=True, skeleton_ids=[1])

    def test_reported_ids_are_capped_but_the_count_is_exact(self):
        error = ReviewItemNodesProtected(list(range(1500, 0, -1)))
        self.assertEqual((1500, 1000, 1), (error.meta["count"], len(error.meta["treenode_ids"]), error.meta["treenode_ids"][0]),
                         "The first 1000 IDs in order, the full count")

    def test_reserved_label_changes_are_refused(self):
        with self.assertRaises(PermissionError, msg="Adding it by hand is refused"):
            reject_reserved_label_change(["axon", REVIEW_ITEM_LABEL], [])
        reject_reserved_label_change(["axon"], ["soma"])
