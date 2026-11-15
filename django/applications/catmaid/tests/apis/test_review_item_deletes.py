import json

from catmaid.models import Treenode
from catmaid.state import make_nocheck_state
from catmaid.tests.review_item_helpers import label_nodes, link_skeleton_to_neuron, unlabel_nodes

from .common import CatmaidApiTestCase, CatmaidApiTransactionTestCase


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


class ReviewItemNeuronDeleteTests(CatmaidApiTransactionTestCase):
    """NGLASS-2386 (spec 8.4): deleting a neuron or skeleton, or replacing it on import, would
    remove every node of it, so any labelled node needs the same confirmation. CATMAID runs these
    deletes in their own transaction, hence a transaction test case."""

    def setUp(self):
        super().setUp()
        self.fake_authentication()
        label_nodes(self.test_project_id, [349])

    def test_neuron_and_skeleton_deletes_are_refused_when_any_node_is_labelled(self):
        self.assertEqual(409, self.client.post(f"/{self.test_project_id}/neuron/2/delete").status_code, "Neuron 2 models skeleton 1")
        self.assertEqual(409, self.client.post(f"/{self.test_project_id}/skeletons/1/delete").status_code, "Same skeleton by its ID")
        self.assertTrue(Treenode.objects.filter(skeleton_id=1).exists(), "Nothing is deleted")

    def test_confirmed_skeleton_delete_goes_through(self):
        response = self.client.post(f"/{self.test_project_id}/skeletons/1/delete", {"confirm_review_items": "true"})
        self.assertEqual(200, response.status_code, "The user confirmed")
        self.assertFalse(Treenode.objects.filter(skeleton_id=1).exists(), "The skeleton is gone")

    def test_every_skeleton_of_a_multi_skeleton_neuron_is_checked(self):
        link_skeleton_to_neuron(self.test_project_id, skeleton_id=361, neuron_id=2)
        label_nodes(self.test_project_id, [367])
        unlabel_nodes(self.test_project_id, [349])
        response = self.client.post(f"/{self.test_project_id}/skeletons/1/delete", {"delete_multi_skeleton_neurons": "true"})
        self.assertEqual(409, response.status_code, "The labelled node is on the neuron's other skeleton")
