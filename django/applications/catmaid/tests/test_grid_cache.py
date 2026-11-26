"""Exercise sparse MsgPack caches and spatial invalidation in CATMAID."""

import json
import math
from unittest.mock import patch

import msgpack

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings

from catmaid import grid_cache
from catmaid.control.node import GridCachedMsgpackNodeProvider, create_node_response
from catmaid.management.commands import catmaid_cache_update_worker
from catmaid.management.commands.catmaid_spatial_update_worker import GridWorker
from catmaid.models import DirtyNodeGridCacheCell, NodeGridCache, Project, ProjectStack, Stack


class GridSizingTests(SimpleTestCase):
    cube = {"dimension": [1000] * 3, "resolution": [1000] * 3}

    @staticmethod
    def coverage(geometry, width):
        return math.prod(
            math.ceil((origin + dimension * resolution) / width) - math.floor(origin / width)
            for dimension, resolution, origin in zip(
                geometry["dimension"], geometry["resolution"], geometry.get("translation", (0, 0, 0))
            )
        )

    def test_real_images_use_longest_side_and_two_nested_caches(self):
        examples = (
            ("841260", [65569, 39908, 19815], [748, 748, 1000], [-374, -374, -500],
             1532676, 3065352, 5, 18),
            ("820994", [66995, 40431, 21847], [748, 748, 1000], [-374, -374, -500],
             1566009, 3132018, 5, 18),
            ("652779", [66803, 40726, 22023], [748, 748, 1000], [-374, -374, -500],
             1561521, 3123042, 5, 18),
            ("191805", [17797, 30801, 10826], [350, 350, 1000], [-175, -175, -500],
             338313, 676626, 5, 27),
            ("human slice", [35483, 24249, 586], [114.4, 114.4, 440], [-57.2, -57.2, -220],
             126852, 253704, 5, 18),
            ("FAFB", [124416, 67072, 7063], [8, 8, 40], [0, 0, 0],
             31104, 62208, 4, 24),
            ("CCF", [1140, 800, 1320], [10000] * 3, [0, 0, 0],
             412500, 825000, 5, 8),
        )
        for name, dimensions, resolution, origin, finest, ceiling, levels, overview in examples:
            with self.subTest(image=name):
                geometry = {"dimension": dimensions, "resolution": resolution, "translation": origin}
                policy = grid_cache.resolve_grid_policy(geometry)
                self.assertEqual(policy.chunk_sizes, tuple(finest * 2**level for level in range(levels)))
                self.assertEqual(policy.cache_sizes, (finest, ceiling))
                self.assertEqual(policy.max_cache_cell_size_nm, ceiling)
                self.assertEqual(self.coverage(geometry, policy.chunk_sizes[-1]), overview)
                self.assertGreater(self.coverage(geometry, policy.chunk_sizes[-2]), 32)
                self.assertEqual(policy.initialization, {
                    "finest_cell_size_nm": finest,
                    "max_cache_cell_size_nm": ceiling,
                    "target_coarsest_chunks": 32,
                })
                if name == "191805":
                    self.assertLess(max(policy.cache_sizes), 4000000)

    def test_documented_fmost_custom_settings_cover_shifted_ome_bounds(self):
        geometry = {"dimension": [17797, 30801, 10826], "resolution": [350, 350, 1000],
                    "translation": [-175, -175, -500]}
        policy = grid_cache.resolve_grid_policy(
            geometry, finest_cell_size_nm=250000, max_cache_cell_size_nm=500000,
            target_coarsest_chunks=64,
        )
        self.assertEqual(policy.cache_sizes, (250000, 500000))
        self.assertEqual(policy.chunk_sizes[-1], 4000000)
        self.assertEqual(self.coverage(geometry, policy.chunk_sizes[-1]), 48)

    def test_rounding_keeps_the_default_largest_cache_nested(self):
        geometry = {"dimension": [65569, 39908, 19815], "resolution": [748, 748, 1000]}
        policy = grid_cache.resolve_grid_policy(geometry)
        self.assertEqual(policy.max_cache_cell_size_nm, 2 * policy.finest_cell_size_nm)
        self.assertIn(policy.max_cache_cell_size_nm, policy.cache_sizes)

    def test_finest_override_does_not_recalculate_the_relative_ceiling(self):
        policy = grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=62500)
        self.assertEqual(policy.finest_cell_size_nm, 62500)
        self.assertEqual(policy.max_cache_cell_size_nm, 62500)
        self.assertEqual(policy.cache_sizes, (62500,))

    def test_explicit_cache_ceiling_is_a_limit_not_an_exact_generated_width(self):
        policy = grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=25000,
                                               max_cache_cell_size_nm=60000)
        self.assertEqual(policy.cache_sizes, (25000, 50000))
        self.assertEqual(policy.max_cache_cell_size_nm, 60000)

    def test_single_level_deduplicates_the_cache_and_reads_all_nodes(self):
        for geometry in (
            {"dimension": [32, 1, 1], "resolution": [1000] * 3},
            {"dimension": [1] * 3, "resolution": [0.1] * 3},
        ):
            with self.subTest(geometry=geometry):
                policy = grid_cache.resolve_grid_policy(geometry)
                self.assertEqual(len(policy.chunk_sizes), 1)
                self.assertEqual(policy.cache_sizes, policy.chunk_sizes)
                self.assertEqual(grid_cache.stack_cache_metadata(policy)["spatial"],
                                 [{"limit": 0, "chunk_size": [policy.finest_cell_size_nm] * 3}])

    def test_cache_selection_keeps_only_endpoints_and_never_a_midpoint(self):
        self.assertEqual(grid_cache.cached_cell_sizes((1,), 4), (1,))
        self.assertEqual(grid_cache.cached_cell_sizes((1, 2), 4), (1, 2))
        self.assertEqual(grid_cache.cached_cell_sizes((1, 2, 4, 8), 4), (1, 4))

    def test_sampling_axis_order_and_translation_do_not_change_nominal_widths(self):
        geometry = {"dimension": [1140, 800, 1320], "resolution": [10000] * 3}
        base = grid_cache.resolve_grid_policy(geometry)
        for changed in (
            {"dimension": [2280, 1600, 2640], "resolution": [5000] * 3},
            {"dimension": [1320, 1140, 800], "resolution": [10000] * 3},
            {**geometry, "translation": [-500, 12000000, 20000000]},
        ):
            with self.subTest(geometry=changed):
                policy = grid_cache.resolve_grid_policy(changed)
                self.assertEqual(policy.finest_cell_size_nm, base.finest_cell_size_nm)
                self.assertEqual(policy.max_cache_cell_size_nm, base.max_cache_cell_size_nm)
        scaled = grid_cache.resolve_grid_policy({**geometry, "resolution": [20000] * 3})
        self.assertEqual(scaled.chunk_sizes, tuple(2 * width for width in base.chunk_sizes))

    def test_translated_default_overviews_fit_within_five_levels(self):
        base = {"dimension": [4096] * 3, "resolution": [1000] * 3}
        for origin in ([0, 0, 0], [-1000] * 3, [4096000] * 3, [-2048000, 0, 0]):
            with self.subTest(origin=origin):
                geometry = {**base, "translation": origin}
                policy = grid_cache.resolve_grid_policy(geometry)
                self.assertLessEqual(len(policy.chunk_sizes), 5)
                self.assertLessEqual(self.coverage(geometry, policy.chunk_sizes[-1]), 32)

    def test_large_volume_uses_relative_widths_instead_of_a_fixed_physical_cap(self):
        geometry = {"dimension": [328000] * 3, "resolution": [1000] * 3}
        policy = grid_cache.resolve_grid_policy(geometry)
        self.assertEqual(policy.finest_cell_size_nm, 10250000)
        self.assertEqual(policy.cache_sizes, (10250000, 20500000))
        self.assertEqual(len(policy.chunk_sizes), 5)

    def test_target_changes_overview_and_cache_candidates_without_changing_finest(self):
        geometry = {"dimension": [32, 2, 1], "resolution": [1000] * 3}
        normal = grid_cache.resolve_grid_policy(geometry)
        tighter_overview = grid_cache.resolve_grid_policy(geometry, target_coarsest_chunks=8)
        single_level = grid_cache.resolve_grid_policy(geometry, target_coarsest_chunks=64)
        self.assertEqual(tighter_overview.finest_cell_size_nm, normal.finest_cell_size_nm)
        self.assertEqual(normal.cache_sizes, (1000, 2000))
        self.assertEqual(tighter_overview.cache_sizes, (1000, 2000))
        self.assertEqual(single_level.cache_sizes, (1000,))
        self.assertGreater(len(tighter_overview.chunk_sizes), len(normal.chunk_sizes))

    def test_explicit_cache_ceiling_can_only_be_lowered_and_cannot_exclude_finest(self):
        for overrides in (
            {"max_cache_cell_size_nm": 62501},
            {"max_cache_cell_size_nm": 25000},
            {"finest_cell_size_nm": 62501},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(grid_cache.GridSizingError):
                grid_cache.resolve_grid_policy(self.cube, **overrides)

    def test_invalid_override_types_and_ranges_are_rejected(self):
        cases = {
            "finest_cell_size_nm": (True, False, 0, -1, 1.5, "1000", 2**31),
            "max_cache_cell_size_nm": (True, False, 0, -1, 1.5, "1000", 2**31),
            "target_coarsest_chunks": (True, False, 0, -1, 1.5, "32",
                                       grid_cache.MAX_TARGET_COARSEST_CHUNKS + 1),
        }
        for name, invalid in cases.items():
            for value in invalid:
                with self.subTest(name=name, value=value), self.assertRaises(grid_cache.GridSizingError):
                    grid_cache.resolve_grid_policy(self.cube, **{name: value})

    @override_settings(NEUROGLASS_CATMAID_TARGET_COARSEST_CHUNKS=0)
    def test_invalid_server_target_is_rejected(self):
        with self.assertRaisesRegex(grid_cache.GridSizingError, "target coarsest chunk count"):
            grid_cache.resolve_grid_policy(self.cube)

    def test_impossible_target_is_reported_before_the_numeric_width_limit(self):
        geometry = {**self.cube, "translation": [-500000] * 3}
        with self.assertRaisesRegex(grid_cache.GridSizingError, "less than 8"):
            grid_cache.resolve_grid_policy(geometry, target_coarsest_chunks=1)
        self.assertLessEqual(self.coverage(geometry, grid_cache.chunk_cell_sizes(
            geometry, target_coarsest_chunks=8)[-1]), 8)

    def test_eight_levels_are_allowed_but_nine_are_not(self):
        policy = grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=3125,
                                               max_cache_cell_size_nm=50000)
        self.assertEqual(len(policy.chunk_sizes), 8)
        with self.assertRaisesRegex(grid_cache.GridSizingError, "more than 8 frontend levels"):
            grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=1562,
                                           max_cache_cell_size_nm=25000)

    def test_cache_aggregation_ratio_eight_is_allowed_and_sixteen_is_rejected(self):
        policy = grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=12500,
                                               max_cache_cell_size_nm=50000)
        self.assertEqual(len(policy.chunk_sizes), 6)
        self.assertEqual(policy.chunk_sizes[-1] // policy.cache_sizes[-1], 8)
        with self.assertRaisesRegex(grid_cache.GridSizingError, "8 times"):
            grid_cache.resolve_grid_policy(self.cube, finest_cell_size_nm=12500,
                                           max_cache_cell_size_nm=25000)

    def test_density_hints_follow_actual_two_cache_selection_and_bucket_prefixes(self):
        cases = (
            ({}, {"dimension": [32, 1, 1], "resolution": [1000] * 3}, [8]),
            ({}, {"dimension": [32, 2, 1], "resolution": [1000] * 3}, [8, 1]),
            ({}, {"dimension": [32, 4, 4], "resolution": [1000] * 3}, [8, 4, 1]),
            ({"finest_cell_size_nm": 62500}, self.cube, [8, 5, 2, 1]),
            ({}, self.cube, [8, 6, 4, 2, 1]),
            ({"finest_cell_size_nm": 12500, "max_cache_cell_size_nm": 50000},
             self.cube, [8, 6, 4, 3, 1, 1]),
            ({"finest_cell_size_nm": 6250, "max_cache_cell_size_nm": 50000},
             self.cube, [8, 6, 5, 4, 2, 1, 1]),
            ({"finest_cell_size_nm": 3125, "max_cache_cell_size_nm": 50000},
             self.cube, [8, 6, 5, 4, 3, 2, 1, 1]),
        )
        for overrides, geometry, prefixes in cases:
            policy = grid_cache.resolve_grid_policy(geometry, **overrides)
            metadata = grid_cache.stack_cache_metadata(policy)
            self.assertEqual(len(policy.chunk_sizes), len(prefixes))
            self.assertEqual(metadata["grid_policy"], policy.initialization)
            for width, prefix, level in zip(policy.chunk_sizes, prefixes, metadata["spatial"]):
                with self.subTest(levels=len(prefixes), width=width):
                    cache = min(policy.cache_sizes, key=lambda size: abs(size**3 - width**3))
                    ratio = width // cache
                    expected = 0 if width == policy.finest_cell_size_nm else (
                        (ratio + 1)**2 * ratio * 500 * sum(bucket**2 for bucket in range(1, prefix + 1))
                    )
                    self.assertEqual(level, {"chunk_size": [width] * 3, "limit": expected})

    def test_coarser_frontend_levels_aggregate_the_largest_cache(self):
        geometry = {"dimension": [65569, 39908, 19815], "resolution": [748, 748, 1000]}
        policy = grid_cache.resolve_grid_policy(geometry)
        self.assertEqual([level["limit"] for level in grid_cache.stack_cache_metadata(policy)["spatial"]],
                         [0, 110000, 45000, 50000])

    def test_negative_and_positive_frontend_boundaries_align_with_selected_caches(self):
        policy = grid_cache.resolve_grid_policy(self.cube)
        for width in policy.chunk_sizes:
            for cache in policy.cache_sizes:
                if cache > width:
                    continue
                for index in (-3, -1, 0, 1, 3):
                    self.assertEqual((index * width) % cache, 0)

    def test_invalid_or_nonfinite_geometry_is_rejected(self):
        for geometry in (
            {},
            {"dimension": [1, 1], "resolution": [1] * 3},
            {"dimension": [1, 1, 0], "resolution": [1] * 3},
            {"dimension": [1, True, 1], "resolution": [1] * 3},
            {"dimension": [1] * 3, "resolution": [1, float("nan"), 1]},
            {"dimension": [1] * 3, "resolution": [1, float("inf"), 1]},
            {"dimension": [1] * 3, "resolution": [1, 0, 1]},
            {"dimension": [1] * 3, "resolution": [1] * 3, "translation": [float("inf"), 0, 0]},
            {"dimension": [2**31 - 1] * 3, "resolution": [1e308] * 3},
            {"dimension": [1] * 3, "resolution": [1] * 3, "translation": [1e308, 0, 0]},
        ):
            with self.subTest(geometry=geometry), self.assertRaises(grid_cache.GridSizingError):
                grid_cache.resolve_grid_policy(geometry)

    def test_unrepresentable_relative_ceiling_and_frontend_width_are_rejected(self):
        with self.assertRaisesRegex(grid_cache.GridSizingError, "relative cache ceiling"):
            grid_cache.resolve_grid_policy({"dimension": [2**31 - 1, 1, 1], "resolution": [100, 1, 1]})
        with self.assertRaisesRegex(grid_cache.GridSizingError, "uint32"):
            grid_cache.resolve_grid_policy({"dimension": [2147483640] * 3, "resolution": [8] * 3})

    def test_unrepresentable_native_indices_are_rejected(self):
        for origin in (10**16, -10**16):
            geometry = {**self.cube, "translation": [origin, 0, 0]}
            with self.subTest(origin=origin), self.assertRaisesRegex(grid_cache.GridSizingError, "index"):
                grid_cache.resolve_grid_policy(geometry)


GEOMETRY = {"dimension": [4096] * 3, "resolution": [4, 4, 40]}


def make_grid_project(title):
    project = Project.objects.create(title=title)
    policy = grid_cache.resolve_grid_policy(GEOMETRY)
    stack = Stack.objects.create(
        title=title,
        dimension=tuple(GEOMETRY["dimension"]),
        resolution=tuple(GEOMETRY["resolution"]),
        metadata=grid_cache.stack_cache_metadata(policy),
    )
    ProjectStack.objects.create(project=project, stack=stack)
    return project, stack, grid_cache.create_grid_caches(project, policy)


class GridCacheTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        get_user_model().objects.create_superuser(username="grid-cache-test")


class SparseGridProviderTests(GridCacheTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        cls.project, cls.stack, cls.grids = make_grid_project("Sparse tracing grid")

    def read(self, *, cache_id=None, bounds=None, **options):
        params = {
            "project_id": self.project.pk, "left": 0, "right": 4096,
            "top": 0, "bottom": 4096, "z1": 0, "z2": 8192,
        }
        params.update(bounds or {})
        return GridCachedMsgpackNodeProvider(cache_id=cache_id).get_tuples(
            params, self.project.pk, **options,
        )

    def test_reads_empty_cells_for_each_managed_cache(self):
        for grid in self.grids:
            with self.subTest(grid=grid.pk):
                data, data_type = self.read(cache_id=grid.pk)
                response = create_node_response(data, {}, "msgpack", None, data_type)
                self.assertEqual(msgpack.unpackb(response.content), [[], [], {}, False, {}])

    def insert_cell(self, grid, cached, z_index=0):
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO node_grid_cache_cell (grid_id, x_index, y_index, z_index, msgpack_data)
                VALUES (%s, 0, 0, %s, ARRAY[%s]::bytea[])
            """, (grid.pk, z_index, msgpack.packb(cached)))

    def test_populated_cells_do_not_need_the_sparse_fallback(self):
        cached = [[[17, None, 1, 2, 3]], [], {}, False, {}]
        self.insert_cell(self.grids[0], cached)
        with patch.object(grid_cache, "has_managed_grid") as detect_sparse:
            data, data_type = self.read()
        detect_sparse.assert_not_called()
        self.assertEqual(data_type, "msgpack")
        self.assertEqual(msgpack.unpackb(data)[:5], cached)

    def test_reads_partial_z_cells_on_either_side_of_zero(self):
        grid = self.grids[0]
        for z_index, z1, z2 in ((0, 1, 5), (-1, -5, -1)):
            with self.subTest(z1=z1, z2=z2):
                cached = [[[17, None, 1, 2, z1]], [], {}, False, {}]
                self.insert_cell(grid, cached, z_index=z_index)
                data, _ = self.read(cache_id=grid.pk, bounds={"z1": z1, "z2": z2})
                self.assertEqual(msgpack.unpackb(data)[:5], cached)

    def test_reads_explicit_grid_instead_of_closer_grid(self):
        first, second = self.grids
        cached = [[[17, None, 1, 2, 3]], [], {}, False, {}]
        self.insert_cell(first, [[], [], {}, False, {}])
        self.insert_cell(second, cached)
        data, _ = self.read(cache_id=second.pk, bounds={"z2": second.cell_depth})
        self.assertEqual(msgpack.unpackb(data)[:5], cached)

    def test_skips_disabled_grid_in_favor_of_enabled_grid(self):
        first, second = self.grids
        NodeGridCache.objects.filter(pk=first.pk).update(enabled=False)
        cached = [[[17, None, 1, 2, 3]], [], {}, False, {}]
        self.insert_cell(second, cached)
        data, _ = self.read(bounds={"z2": second.cell_depth})
        self.assertEqual(msgpack.unpackb(data)[:5], cached)

    def test_reads_cached_format_added_to_an_existing_grid(self):
        grid = self.grids[0]
        # The cache builder can add data of another format to an existing grid.
        NodeGridCache.objects.filter(pk=grid.pk).update(has_msgpack_data=False, has_json_data=True)
        cached = [[[17, None, 1, 2, 3]], [], {}, False, {}]
        self.insert_cell(grid, cached)
        for cache_id in (None, grid.pk):
            with self.subTest(cache_id=cache_id):
                data, _ = self.read(cache_id=cache_id, bounds={"z2": grid.cell_depth})
                self.assertIsNotNone(data)
                self.assertEqual(msgpack.unpackb(data)[:5], cached)

    def test_legacy_single_level_grids_are_still_sparse(self):
        first, second = self.grids
        NodeGridCache.objects.filter(pk=second.pk).delete()
        NodeGridCache.objects.filter(pk=first.pk).update(n_lod_levels=1)
        self.assertEqual(self.read()[0], [[], [], {}, False, {}])

    def test_falls_back_when_project_has_no_managed_cache(self):
        self.stack.metadata = {"read_only": False}
        self.stack.save(update_fields=["metadata"])
        self.assertEqual(self.read(), (None, None))

    def test_falls_back_when_enabled_msgpack_cache_is_unavailable(self):
        for field in ("enabled", "has_msgpack_data"):
            with self.subTest(field=field):
                NodeGridCache.objects.filter(project=self.project).update(**{field: False})
                self.assertEqual(self.read(), (None, None))
                NodeGridCache.objects.filter(project=self.project).update(**{field: True})

    def test_falls_back_for_an_explicit_unmatched_cache(self):
        self.assertEqual(self.read(cache_id=-1), (None, None))

    def test_includes_explicit_nodes_and_connectors_on_an_empty_read(self):
        extra = [[[17, None, 1, 2, 3]], [], {}, False, {}]
        with patch("catmaid.control.node.get_extra_nodes", return_value=(extra, "json")) as get_extra:
            data, data_type = self.read(
                explicit_treenode_ids=(17,), explicit_connector_ids=(23,), with_origin=True,
            )
        response = create_node_response(data, {}, "msgpack", None, data_type)
        self.assertEqual(msgpack.unpackb(response.content), extra)
        self.assertEqual(get_extra.call_args.args[1:], (self.project.pk, (17,), (23,), False, "used", True))

    def test_includes_requested_relation_map_on_an_empty_read(self):
        with patch("catmaid.control.node.get_relation_to_id_map", return_value={"presynaptic_to": 7}):
            data, data_type = self.read(with_relation_map="all")
        response = create_node_response(data, {}, "msgpack", None, data_type)
        self.assertEqual(msgpack.unpackb(response.content, strict_map_key=False)[4], {7: "presynaptic_to"})

    def test_json_response_includes_requested_relation_map_on_an_empty_read(self):
        with patch("catmaid.control.node.get_relation_to_id_map", return_value={"presynaptic_to": 7}):
            data, data_type = self.read(with_relation_map="all")
        response = create_node_response(data, {}, "json", None, data_type)
        self.assertEqual(json.loads(response.content), [[], [], {}, False, {"7": "presynaptic_to"}])


class DirtyGridStartupTests(TransactionTestCase):
    def test_worker_re_notifies_dirty_cells_from_multiple_batches_after_listening(self):
        get_user_model().objects.create_superuser(username="dirty-grid-startup-test")
        _, _, (grid, _) = make_grid_project("Restart backlog")
        DirtyNodeGridCacheCell.objects.bulk_create([
            DirtyNodeGridCacheCell(grid=grid, x_index=x, y_index=0, z_index=-1)
            for x in range(12)
        ])
        original_times = dict(DirtyNodeGridCacheCell.objects.values_list("id", "invalidation_time"))
        command = catmaid_cache_update_worker.Command()
        payloads = []

        def consume_notifications():
            connection.connection.poll()
            notifications = command.filter_notifies()
            if not notifications:
                raise InterruptedError
            payloads.extend(json.loads(notification.payload) for notification in notifications)

        with patch.object(catmaid_cache_update_worker, "signal"), \
                patch.object(command, "wait_and_queue", side_effect=consume_notifications):
            try:
                command.handle(grid_cache=True, delay=0)
            finally:
                with connection.cursor() as cursor:
                    cursor.execute('UNLISTEN "catmaid.dirty-cache"')

        self.assertCountEqual(payloads, [
            {"grid_id": grid.pk, "x": x, "y": 0, "z": -1} for x in range(12)
        ])
        self.assertEqual(dict(DirtyNodeGridCacheCell.objects.values_list("id", "invalidation_time")),
                         original_times)


class SpatialWorkerGridDiscoveryTests(GridCacheTestCase):
    @staticmethod
    def update(worker, *projects):
        events = ({"project_id": project.pk, "type": "point", "p": [-500, 0, 250]} for project in projects)
        with connection.cursor() as cursor:
            worker.update(events, cursor)

    @override_settings(NODE_PROVIDERS=["cached_msgpack_grid"])
    def test_first_edit_invalidates_grids_provisioned_after_worker_start(self):
        worker = GridWorker()
        project, _, grids = make_grid_project("Provisioned after startup")
        self.update(worker, project)
        self.assertCountEqual(worker.grid_caches, grids)
        self.assertEqual(worker.cache_project_ids, {project.pk})
        self.assertEqual(worker.updatesReceived, 1)
        self.assertCountEqual(
            DirtyNodeGridCacheCell.objects.filter(grid__project=project).values_list("grid_id", flat=True),
            [grid.pk for grid in grids],
        )

    @override_settings(NODE_PROVIDERS=["cached_msgpack_grid"])
    def test_refresh_scopes_to_batch_projects_and_drops_disabled_grids(self):
        project, _, grids = make_grid_project("One tracing space")
        other, _, _ = make_grid_project("Other tracing space")
        worker = GridWorker()
        self.update(worker, other)
        self.assertEqual(worker.cache_project_ids, {other.pk})
        NodeGridCache.objects.filter(pk=grids[0].pk).update(enabled=False)
        self.update(worker, project)
        self.assertCountEqual(worker.grid_caches, grids[1:])
        self.assertEqual(worker.cache_project_ids, {project.pk})
        self.assertEqual(worker.updatesReceived, 2)

    def test_preserves_provider_scope_and_deduplicates_overlaps(self):
        project, _, grids = make_grid_project("Allowed tracing space")
        other, _, _ = make_grid_project("Excluded tracing space")
        for providers, expected in (
            ([("cached_msgpack_grid", {"cache_id": grids[0].pk})], grids[:1]),
            ([("cached_msgpack_grid", {"project_id": project.pk})], grids),
            ([
                ("cached_msgpack_grid", {"cache_id": grids[0].pk}),
                ("cached_msgpack_grid", {"project_id": project.pk}),
            ], grids),
            (["postgis3d"], []),
        ):
            with self.subTest(providers=providers), override_settings(NODE_PROVIDERS=providers):
                worker = GridWorker()
                self.update(worker, project, other)
                self.assertCountEqual(worker.grid_caches, expected)
