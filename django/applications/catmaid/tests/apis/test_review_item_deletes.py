import json

from catmaid.models import Treenode
from catmaid.state import make_nocheck_state
from catmaid.tests.review_item_helpers import label_nodes

from .common import CatmaidApiTestCase


class ReviewItemDeleteTests(CatmaidApiTestCase):
    """NGLASS-2386 (spec 8.4): deleting a node a review item points at needs the viewer's
    confirmation, from any client."""

    def setUp(self):
        super().setUp()
        self.fake_authentication()
        label_nodes(self.test_project_id, [349])

    def delete_node(self, treenode_id, **extra):
        return self.client.post(f"/{self.test_project_id}/treenode/delete",
                                {"treenode_id": treenode_id, "state": make_nocheck_state(), **extra})

    def test_labelled_node_is_refused_with_its_id(self):
        response = self.delete_node(349)
        body = json.loads(response.content.decode())
        self.assertEqual((409, "ReviewItemNodesProtected", [349]), (response.status_code, body["type"], body["meta"]["treenode_ids"]),
                         "The viewer turns this into a confirmation")
        self.assertTrue(Treenode.objects.filter(pk=349).exists(), "Nothing is deleted")

    def test_confirmed_delete_goes_through(self):
        self.assertEqual(200, self.delete_node(349, confirm_review_items="true").status_code, "The user confirmed")
        self.assertFalse(Treenode.objects.filter(pk=349).exists(), "The node is gone")

    def test_unlabelled_nodes_are_unaffected(self):
        self.assertEqual(200, self.delete_node(13).status_code, "Other nodes delete as before")
