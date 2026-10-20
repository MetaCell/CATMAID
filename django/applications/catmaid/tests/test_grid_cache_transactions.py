"""Exercise cell publication races using independent PostgreSQL connections."""

from concurrent.futures import Future
from contextlib import contextmanager
import json
from queue import Queue
from threading import Event, Thread
from time import monotonic
from unittest.mock import patch

import msgpack

from django.db import connection, connections, transaction
from django.test import TransactionTestCase, override_settings

from catmaid.control.grid_cache import locked_grid_cell
from catmaid.management.commands.catmaid_cache_update_worker import GridWorker
from catmaid.models import NodeGridCache


class GridCacheTransactionTests(TransactionTestCase):
    """Use committed fixtures so another connection can observe each transition."""

    def setUp(self):
        super().setUp()
        if connection.vendor != 'postgresql':
            self.skipTest('Grid cache publication requires PostgreSQL')
        # No tracing setup is needed: these tests exercise cache rows and locks.
        # A project comment stands in for source data read during generation.
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO project (title, comment) VALUES (%s, %s) RETURNING id",
                    ['Grid cache transaction test', '1'])
            self.project_id = cursor.fetchone()[0]
        self.grid = NodeGridCache.objects.create(project_id=self.project_id,
                cell_width=100, cell_height=100, cell_depth=100,
                hidden_last_editor_id=None, has_json_data=True,
                has_json_text_data=True, has_msgpack_data=True)
        self.cell = (self.grid.id, 0, 0, 0)
        with connection.cursor() as cursor:
            self.publish(cursor, 1)

    @contextmanager
    def background(self, action, release=None):
        """Give each thread its own Django connection and bound every DB wait."""
        result = Future()
        backend = Queue()
        alias = connection.alias

        def run():
            db = connections[alias]
            try:
                with db.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '10s'")
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute('SELECT pg_backend_pid()')
                    backend.put(cursor.fetchone()[0])
                result.set_result(action(db))
            except BaseException as error:
                result.set_exception(error)
                backend.put(None)
            finally:
                db.close()

        thread = Thread(target=run, daemon=True)
        thread.start()
        try:
            pid = backend.get(timeout=10)
            if pid is None:
                result.result(timeout=10)
            yield result, pid
        finally:
            if release is not None:
                release.set()
            thread.join(timeout=12)
            self.assertFalse(thread.is_alive(), 'Cache test connection did not finish')

    def wait_for(self, event):
        self.assertTrue(event.wait(timeout=10), 'Cache test did not reach its synchronization point')

    def wait_for_blocker(self, waiting_pid, blocking_pid):
        """Observe a real server-side wait instead of guessing with a sleep."""
        deadline = monotonic() + 5
        while monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute('SELECT %s = ANY(pg_blocking_pids(%s))',
                        [blocking_pid, waiting_pid])
                if cursor.fetchone()[0]:
                    return
            Event().wait(0.01)
        self.fail(f'Backend {waiting_pid} did not block on backend {blocking_pid}')

    def publish(self, cursor, version, cell=None, fields=None):
        """Use separate writes, like update_grid_cell's payload format writes."""
        cell = self.cell if cell is None else cell
        data = {'version': version}
        payloads = {
            'json_data': ('jsonb', json.dumps(data)),
            'json_text_data': ('text', json.dumps(data)),
            'msgpack_data': ('bytea', msgpack.packb(data)),
        }
        for field in (payloads if fields is None else fields):
            cast, payload = payloads[field]
            cursor.execute(f'''
                INSERT INTO node_grid_cache_cell
                    (grid_id, x_index, y_index, z_index, {field})
                VALUES (%s, %s, %s, %s, ARRAY[%s::{cast}])
                ON CONFLICT (grid_id, x_index, y_index, z_index)
                DO UPDATE SET {field}=EXCLUDED.{field}
            ''', [*cell, payload])

    def snapshot(self, cell=None):
        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT json_data[1]::text, json_text_data[1], msgpack_data[1], update_time
                FROM node_grid_cache_cell
                WHERE grid_id=%s AND x_index=%s AND y_index=%s AND z_index=%s
            ''', self.cell if cell is None else cell)
            row = cursor.fetchone()
        return (json.loads(row[0]), json.loads(row[1]),
                msgpack.unpackb(bytes(row[2]), strict_map_key=False), row[3])

    def invalidate(self, db=connection, preserve_timestamp=False):
        with db.cursor() as cursor:
            # An UPDATE with an unchanged timestamp still changes xmin. This
            # reproduces invalidations that timestamp comparisons cannot order.
            timestamp = 'dirty_node_grid_cache_cell.invalidation_time' if preserve_timestamp else 'now()'
            cursor.execute(f'''
                INSERT INTO dirty_node_grid_cache_cell
                    (grid_id, x_index, y_index, z_index)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (grid_id, x_index, y_index, z_index)
                DO UPDATE SET invalidation_time={timestamp}
                RETURNING id, xmin::text, invalidation_time
            ''', self.cell)
            return cursor.fetchone()

    def dirty(self):
        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT id, xmin::text, invalidation_time
                FROM dirty_node_grid_cache_cell
                WHERE grid_id=%s AND x_index=%s AND y_index=%s AND z_index=%s
            ''', self.cell)
            return cursor.fetchone()

    def test_same_cell_serializes_source_reads_and_records_time_after_waiting(self):
        first_read = Event()
        release_first = Event()
        second_read = Event()

        def refresh_first(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                cursor.execute('SELECT comment FROM project WHERE id=%s', [self.project_id])
                source = int(cursor.fetchone()[0])
                first_read.set()
                self.wait_for(release_first)
                self.publish(cursor, source)

        def refresh_second(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, receipt):
                second_read.set()
                cursor.execute('SELECT comment, transaction_timestamp() FROM project WHERE id=%s',
                        [self.project_id])
                source, transaction_start = cursor.fetchone()
                self.publish(cursor, int(source))
            return receipt, transaction_start

        with self.background(refresh_first, release_first) as (first, first_pid):
            self.wait_for(first_read)
            with connection.cursor() as cursor:
                cursor.execute("UPDATE project SET comment='2' WHERE id=%s", [self.project_id])
            with self.background(refresh_second) as (second, second_pid):
                self.wait_for_blocker(second_pid, first_pid)
                self.assertFalse(second_read.is_set())
                with connection.cursor() as cursor:
                    cursor.execute('SELECT clock_timestamp()')
                    before_release = cursor.fetchone()[0]
                release_first.set()
                first.result(timeout=10)
                receipt, transaction_start = second.result(timeout=10)
        self.assertLess(transaction_start, before_release)
        self.assertGreaterEqual(receipt['generated_at'], before_release)
        self.assertEqual(self.snapshot()[:3], ({'version': 2},) * 3)
        self.assertEqual(self.snapshot()[3], receipt['generated_at'])

    def test_different_cells_can_refresh_while_one_cell_is_locked(self):
        entered = Event()
        release = Event()
        other_cell = (self.grid.id, 1, 0, 0)

        def hold_first(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                entered.set()
                self.wait_for(release)
                self.publish(cursor, 2)

        def refresh_other(db):
            with locked_grid_cell(*other_cell, connection=db) as (cursor, _receipt):
                self.publish(cursor, 3, cell=other_cell)

        with self.background(hold_first, release) as (first, _pid):
            self.wait_for(entered)
            with self.background(refresh_other) as (second, _pid):
                second.result(timeout=5)
            self.assertFalse(first.done())
            self.assertEqual(self.snapshot(other_cell)[:3], ({'version': 3},) * 3)
            release.set()
            first.result(timeout=10)

    def test_updated_invalidation_survives_even_with_the_same_timestamp(self):
        original = self.invalidate()
        entered = Event()
        release = Event()

        def refresh(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                entered.set()
                self.wait_for(release)
                self.publish(cursor, 2)

        with self.background(refresh, release) as (result, _pid):
            self.wait_for(entered)
            updated = self.invalidate(preserve_timestamp=True)
            self.assertEqual(updated[0], original[0])
            self.assertNotEqual(updated[1], original[1])
            self.assertEqual(updated[2], original[2])
            release.set()
            result.result(timeout=10)
        self.assertEqual(self.dirty(), updated)
        # A subsequent successful refresh acknowledges precisely that version.
        with locked_grid_cell(*self.cell) as (cursor, _receipt):
            self.publish(cursor, 3)
        self.assertIsNone(self.dirty())

    def test_invalidation_inserted_during_generation_survives(self):
        entered = Event()
        release = Event()

        def refresh(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                entered.set()
                self.wait_for(release)
                self.publish(cursor, 2)

        with self.background(refresh, release) as (result, _pid):
            self.wait_for(entered)
            inserted = self.invalidate()
            release.set()
            result.result(timeout=10)
        self.assertEqual(self.dirty(), inserted)

    def test_invalidation_committing_while_acknowledgement_waits_survives(self):
        original = self.invalidate()
        invalidated = Event()
        release_edit = Event()

        def edit(db):
            with transaction.atomic(using=db.alias):
                updated = self.invalidate(db, preserve_timestamp=True)
                invalidated.set()
                self.wait_for(release_edit)
            return updated

        def refresh(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                self.publish(cursor, 2)

        with self.background(edit, release_edit) as (editing, editing_pid):
            self.wait_for(invalidated)
            with self.background(refresh) as (refreshing, refreshing_pid):
                # SELECT sees the old committed version. DELETE then waits on
                # the edit's row lock and must recheck xmin after it commits.
                self.wait_for_blocker(refreshing_pid, editing_pid)
                release_edit.set()
                updated = editing.result(timeout=10)
                refreshing.result(timeout=10)
        self.assertNotEqual(original[1], updated[1])
        self.assertEqual(self.dirty(), updated)

    def test_readers_see_all_old_formats_until_successful_commit(self):
        original = self.snapshot()
        original_dirty = self.invalidate()
        partial = Event()
        release = Event()

        def refresh(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                self.publish(cursor, 2, fields=['json_data'])
                partial.set()
                self.wait_for(release)
                self.publish(cursor, 2, fields=['json_text_data', 'msgpack_data'])

        with self.background(refresh, release) as (result, _pid):
            self.wait_for(partial)
            self.assertEqual(self.snapshot(), original)
            self.assertEqual(self.dirty(), original_dirty)
            release.set()
            result.result(timeout=10)
        self.assertEqual(self.snapshot()[:3], ({'version': 2},) * 3)
        self.assertIsNone(self.dirty())

    def test_failed_generation_rolls_back_payloads_and_keeps_invalidation(self):
        original = self.snapshot()
        original_dirty = self.invalidate()
        written = Event()
        release = Event()

        def refresh(db):
            with locked_grid_cell(*self.cell, connection=db) as (cursor, _receipt):
                self.publish(cursor, 2)
                written.set()
                self.wait_for(release)
                raise ValueError('Generation failed')

        with self.background(refresh, release) as (result, _pid):
            self.wait_for(written)
            self.assertEqual(self.snapshot(), original)
            release.set()
            with self.assertRaisesRegex(ValueError, 'Generation failed'):
                result.result(timeout=10)
        self.assertEqual(self.snapshot(), original)
        self.assertEqual(self.dirty(), original_dirty)
        # Rollback releases the cell lock, so a later refresh can recover.
        with locked_grid_cell(*self.cell) as (cursor, _receipt):
            self.publish(cursor, 3)
        self.assertIsNone(self.dirty())

    @override_settings(SPATIAL_UPDATE_CLIENT_NOTIFICATIONS=True)
    def test_worker_locks_before_query_and_notifies_after_commit(self):
        self.invalidate()
        node_data = [[[7, None, 1, 2, 3, 5, 1, 8, 0, 1]], [], {}, False, {}]

        def query(*_args, **_kwargs):
            self.assertTrue(connection.in_atomic_block)
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT count(*) FROM pg_locks
                    WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted
                ''')
                self.assertEqual(cursor.fetchone()[0], 1)
            return node_data

        def notified(*_args, **_kwargs):
            self.assertFalse(connection.in_atomic_block)
            self.assertIsNone(self.dirty())
            payloads = self.snapshot()[:3]
            self.assertEqual(payloads[0], payloads[1])
            self.assertEqual(payloads[0], payloads[2])
            self.assertEqual(payloads[0][0][0][0], 7)

        with patch('catmaid.control.node._node_list_tuples_query', side_effect=query), \
                patch('catmaid.management.commands.catmaid_cache_update_worker.msg_all',
                    side_effect=notified) as notify:
            GridWorker().update([{'grid_id': self.grid.id, 'x': 0, 'y': 0, 'z': 0}], None)
        notify.assert_called_once()

    @override_settings(SPATIAL_UPDATE_CLIENT_NOTIFICATIONS=True)
    def test_worker_serialization_failure_rolls_back_earlier_format_writes(self):
        original = self.snapshot()
        original_dirty = self.invalidate()
        node_data = [[[7, None, 1, 2, 3, 5, 1, 8, 0, 1]], [], {}, False, {}]
        with patch('catmaid.control.node._node_list_tuples_query', return_value=node_data), \
                patch('catmaid.control.node.msgpack.packb', side_effect=ValueError('Invalid payload')), \
                patch('catmaid.management.commands.catmaid_cache_update_worker.msg_all') as notify:
            with self.assertRaisesRegex(ValueError, 'Invalid payload'):
                GridWorker().update([{'grid_id': self.grid.id, 'x': 0, 'y': 0, 'z': 0}], None)
        self.assertEqual(self.snapshot(), original)
        self.assertEqual(self.dirty(), original_dirty)
        notify.assert_not_called()
