from catmaid.models import ClassInstance, Treenode
from catmaid.review_items import REVIEW_ITEM_LABEL, labelled_treenode_ids
from catmaid.tests.review_item_helpers import label_nodes

from .common import CatmaidApiTestCase


class ReviewItemLabelTests(CatmaidApiTestCase):
    """NGLASS-2386 (spec 8.4): only imports add or remove the review-item label; users keep
    editing their own tags on the same nodes."""

    def setUp(self):
        super().setUp()
        self.fake_authentication()
        label_nodes(self.test_project_id, [349])

    def update(self, node, tags, **extra):
        return self.client.post(f"/{self.test_project_id}/label/treenode/{node}/update", {"tags": tags, **extra})

    def test_adding_or_dropping_the_label_by_hand_is_refused(self):
        self.assertEqual(403, self.update(13, f"axon,{REVIEW_ITEM_LABEL}").status_code, "Adding it by hand is refused")
        self.assertEqual(403, self.update(349, "axon").status_code, "Replacing the tag set would drop it")
        self.assertEqual(403, self.client.post(f"/{self.test_project_id}/label/treenode/349/remove",
                                               {"tag": REVIEW_ITEM_LABEL}).status_code, "Removing it is refused")
        self.assertEqual([349], labelled_treenode_ids(self.test_project_id, treenode_ids=[13, 349]), "Nothing changed")

    def test_other_tags_on_a_labelled_node_still_edit(self):
        self.assertEqual(200, self.update(349, f"axon,{REVIEW_ITEM_LABEL}").status_code, "Keeping it while adding a tag is fine")
        self.assertEqual(200, self.update(349, "soma", delete_existing="false").status_code,
                         "Without delete_existing nothing is removed")
        self.assertEqual([349], labelled_treenode_ids(self.test_project_id, treenode_ids=[349]), "The label stays")

    def test_the_annotation_endpoint_cannot_delete_the_label_or_a_skeleton(self):
        label_id = ClassInstance.objects.get(project_id=self.test_project_id, name=REVIEW_ITEM_LABEL).id
        self.client.post(f"/{self.test_project_id}/annotations/{label_id}/remove")
        self.assertEqual([349], labelled_treenode_ids(self.test_project_id, treenode_ids=[349]),
                         "Removing an 'annotation' by the label's ID must not delete the label and its links")
        self.client.post(f"/{self.test_project_id}/annotations/1/remove")
        self.assertTrue(Treenode.objects.filter(skeleton_id=1).exists(),
                        "Removing an 'annotation' by a skeleton's ID must not delete the skeleton")
