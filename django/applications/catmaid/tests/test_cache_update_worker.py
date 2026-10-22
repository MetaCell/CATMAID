from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from catmaid.management.commands import catmaid_cache_update_worker as command


@override_settings(NODE_LIST_MAXIMUM_COUNT=3500,
        SPATIAL_UPDATE_CLIENT_NOTIFICATIONS=False)
class GridWorkerSkeletonFilterTests(SimpleTestCase):
    """Each optional skeleton filter must reach cell generation independently."""

    def check_filters(self, largest, last_edited, expected):
        grid = SimpleNamespace(
            id=11, project_id=123, cell_width=10, cell_height=20, cell_depth=30,
            ordering=None, n_largest_skeletons_limit=largest,
            n_last_edited_skeletons_limit=last_edited, hidden_last_editor_id=None,
            allow_empty=False, n_lod_levels=1, lod_min_bucket_size=500,
            lod_strategy='linear', has_json_data=False, has_json_text_data=False,
            has_msgpack_data=True)

        with patch.object(command, 'NodeGridCache') as grids, \
                patch.object(command, 'DirtyNodeGridCacheCell'), \
                patch.object(command, 'connection'), \
                patch.object(command, 'Postgis3dNodeProvider'), \
                patch.object(command, 'update_grid_cell', return_value=True) as update:
            grids.objects.filter.return_value = [grid]
            command.GridWorker().update(
                [{'grid_id': grid.id, 'x': 1, 'y': 2, 'z': 3}], None)

        update.assert_called_once()
        self.assertEqual(update.call_args.args[8], {
            'project_id': 123, 'limit': 3500, 'ordering': None, **expected,
        })

    def test_largest_filter_without_last_edited_filter(self):
        self.check_filters(5, None, {'n_largest_skeletons_limit': 5})

    def test_last_edited_filter_without_largest_filter(self):
        self.check_filters(None, 3, {'n_last_edited_skeletons_limit': 3})

    def test_both_filters(self):
        self.check_filters(5, 3, {
            'n_largest_skeletons_limit': 5, 'n_last_edited_skeletons_limit': 3,
        })

    def test_no_filters(self):
        self.check_filters(None, None, {})
