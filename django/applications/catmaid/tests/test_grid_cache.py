from unittest import TestCase
from unittest.mock import Mock, patch

from catmaid.control.node import update_grid_cell


class GridCellBoundsTests(TestCase):
    def test_rectangular_cells_query_the_full_height(self):
        cases = (
            ((0, 0, 0), (0, 10, 0, 20, 0, 30)),
            ((1, 2, 3), (10, 20, 40, 60, 90, 120)),
            ((-1, -2, -3), (-10, 0, -40, -20, -90, -60)),
        )
        for indices, bounds in cases:
            with self.subTest(indices=indices):
                params = {}
                provider, cursor = Mock(), Mock()
                with patch('catmaid.control.node._node_list_tuples_query',
                        return_value=[[], []]) as query:
                    update_grid_cell(1, 2, *indices, 10, 20, 30, params,
                            False, 1, 500, 'linear', True, False, False,
                            provider=provider, cursor=cursor)

                # Check the spatial query itself without needing a database.
                self.assertEqual(params, dict(zip(
                        ('left', 'right', 'top', 'bottom', 'z1', 'z2'), bounds)))
                query.assert_called_once_with(params, 1, provider, include_labels=True)
