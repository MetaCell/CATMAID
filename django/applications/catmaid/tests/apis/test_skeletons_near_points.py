# -*- coding: utf-8 -*-

import json

from django.contrib.auth.models import User
from guardian.shortcuts import assign_perm

from catmaid.control.skeleton_proximity import NEAR_POINTS_MAX
from catmaid.models import Treenode

from .common import CatmaidApiTestCase


class SkeletonsNearPointsApiTests(CatmaidApiTestCase):
    def setUp(self):
        super().setUp()
        self.url = '/%d/skeletons/near-points' % self.test_project_id
        self.node_7 = Treenode.objects.get(pk=7)
        self.node_405 = Treenode.objects.get(pk=405)

    def post(self, points, radius):
        return self.client.post(
                self.url,
                json.dumps({'points': points, 'radius': radius}),
                content_type='application/json')

    def results(self, response):
        self.assertStatus(response)
        return json.loads(response.content.decode('utf-8'))['results']

    def pos(self, node):
        return [node.location_x, node.location_y, node.location_z]

    def test_returns_every_skeleton_in_range_with_its_nearest_node(self):
        self.fake_authentication()
        results = self.results(self.post([self.pos(self.node_7)], 1e6))
        self.assertEqual(1, len(results))
        by_skeleton = {r['skeleton_id']: r for r in results[0]}
        self.assertIn(1, by_skeleton)
        self.assertIn(373, by_skeleton)
        self.assertEqual(7, by_skeleton[1]['treenode_id'])
        self.assertEqual(0, by_skeleton[1]['distance'])
        self.assertEqual(self.node_7.location_x, by_skeleton[1]['x'])
        self.assertEqual(self.node_7.location_y, by_skeleton[1]['y'])
        self.assertEqual(self.node_7.location_z, by_skeleton[1]['z'])
        distances = [r['distance'] for r in results[0]]
        self.assertEqual(sorted(distances), distances)
        self.assertEqual(1, results[0][0]['skeleton_id'])

    def test_point_far_from_everything_returns_empty_list(self):
        self.fake_authentication()
        results = self.results(self.post([[-1e7, -1e7, -1e7]], 10))
        self.assertEqual([[]], results)

    def test_results_keep_input_order(self):
        self.fake_authentication()
        results = self.results(self.post(
                [self.pos(self.node_405), self.pos(self.node_7)], 1))
        self.assertEqual(2, len(results))
        self.assertEqual(373, results[0][0]['skeleton_id'])
        self.assertEqual(405, results[0][0]['treenode_id'])
        self.assertEqual(1, results[1][0]['skeleton_id'])
        self.assertEqual(7, results[1][0]['treenode_id'])

    def test_root_id_is_current_root(self):
        self.fake_authentication()
        results = self.results(self.post([self.pos(self.node_7)], 1e6))
        self.assertTrue(results[0])
        for entry in results[0]:
            response = self.client.get('/%d/skeletons/%d/root' % (
                    self.test_project_id, entry['skeleton_id']))
            self.assertStatus(response)
            expected = json.loads(response.content.decode('utf-8'))['root_id']
            self.assertEqual(expected, entry['root_id'])

    def test_rejects_more_than_5000_points(self):
        self.fake_authentication()
        self.assertEqual(5000, NEAR_POINTS_MAX)
        response = self.post([[0, 0, 0]] * (NEAR_POINTS_MAX + 1), 10)
        self.assertEqual(400, response.status_code)

    def test_rejects_non_positive_radius(self):
        self.fake_authentication()
        for radius in (0, -5):
            response = self.post([[0, 0, 0]], radius)
            self.assertEqual(400, response.status_code)

    def test_rejects_malformed_points(self):
        self.fake_authentication()
        for points in ([[1, 2]], 'x', [['a', 'b', 'c']]):
            response = self.post(points, 10)
            self.assertEqual(400, response.status_code)

    def test_rejects_non_object_body(self):
        self.fake_authentication()
        for payload in ([], None, 'points'):
            with self.subTest(payload=payload):
                response = self.client.post(self.url, json.dumps(payload),
                        content_type='application/json')
                self.assertEqual(400, response.status_code)

    def test_rejects_unusable_spatial_numbers(self):
        self.fake_authentication()
        for value in (True, 10**400, 1e308):
            with self.subTest(value=value):
                self.assertEqual(400, self.post([[value, 0, 0]], 10).status_code)
                self.assertEqual(400, self.post([[0, 0, 0]], value).status_code)

    def test_browse_only_user_can_read(self):
        user = User.objects.create_user('browse_only', password='pw')
        assign_perm('can_browse', user, self.test_project)
        self.client.login(username='browse_only', password='pw')
        results = self.results(self.post([self.pos(self.node_7)], 1))
        self.assertEqual(7, results[0][0]['treenode_id'])
