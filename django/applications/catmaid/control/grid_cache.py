"""Coordinate grid-cache writes and invalidation replay."""

from contextlib import contextmanager
import hashlib


DIRTY_CACHE_CHANNEL = 'catmaid.dirty-cache'
DEFAULT_REPLAY_BATCH_SIZE = 10


def requeue_dirty_grid_cells(cursor, batch_size=DEFAULT_REPLAY_BATCH_SIZE):
    """Re-notify persisted dirty cells, yielding after each batch."""
    cursor.execute('SELECT MAX(id) FROM dirty_node_grid_cache_cell')
    last_id, = cursor.fetchone()
    if last_id is None:
        return

    after_id = 0
    while after_id < last_id:
        cursor.execute("""
            WITH batch AS (
                SELECT id, grid_id, x_index, y_index, z_index
                FROM dirty_node_grid_cache_cell
                WHERE id > %s AND id <= %s
                ORDER BY id
                LIMIT %s
            )
            SELECT id, pg_notify(%s, json_build_object(
                'grid_id', grid_id, 'x', x_index, 'y', y_index, 'z', z_index
            )::text)
            FROM batch
            ORDER BY id
        """, [after_id, last_id, batch_size, DIRTY_CACHE_CHANNEL])
        rows = cursor.fetchall()
        if not rows:
            return
        after_id = rows[-1][0]
        yield


def cell_lock_id(grid_id, x, y, z):
    """Use the same transaction advisory lock in importers and cache workers."""
    value = f'catmaid.grid-cell:{grid_id}:{x}:{y}:{z}'.encode('ascii')
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'big', signed=True)


@contextmanager
def locked_grid_cell(grid_id, x, y, z, *, connection=None,
        atomic=None, lock_timeout_ms=None, statement_timeout_ms=None):
    """Publish all formats atomically and acknowledge only an observed invalidation.

    Spatial notifications may arrive while a cell is being generated. Compare
    the dirty row's MVCC version, not just its timestamp (``now()`` is the
    invalidator's transaction start time), so a newer invalidation survives.

    Batch imports rebuild after loading, while normal workers can also refresh
    cells. Sharing this transaction and lock prevents their format writes from
    interleaving. Readers see the previous committed payload when one exists;
    the full grid rebuild still requires the target layer to remain inactive.
    """
    if connection is None or atomic is None:
        from django.db import connection as default_connection, transaction
        connection = connection if connection is not None else default_connection
        atomic = atomic or (lambda: transaction.atomic(using=connection.alias))
    # Every format and its invalidation acknowledgement must share one cell's
    # commit or rollback, independently of a caller's longer-running operation.
    if connection.in_atomic_block:
        raise RuntimeError('Grid cell refresh requires its own transaction')
    coordinates = [grid_id, x, y, z]
    with atomic(), connection.cursor() as cursor:
        for name, timeout in (('lock_timeout', lock_timeout_ms),
                ('statement_timeout', statement_timeout_ms)):
            if timeout is not None:
                cursor.execute('SELECT set_config(%s, %s, true)',
                        [name, f'{int(timeout)}ms'])
        cursor.execute('SELECT pg_advisory_xact_lock(%s)',
                [cell_lock_id(*coordinates)])
        cursor.execute('''
            SELECT id, xmin::text FROM dirty_node_grid_cache_cell
            WHERE grid_id=%s AND x_index=%s AND y_index=%s AND z_index=%s
        ''', coordinates)
        invalidation = cursor.fetchone()
        cursor.execute('SELECT clock_timestamp()')
        started_at = cursor.fetchone()[0]
        receipt = {'generated_at': started_at, 'present': False}
        yield cursor, receipt
        # update_grid_cell uses now(), which can predate waiting for the lock.
        # Record the actual query barrier, shared by every payload format.
        cursor.execute('''
            UPDATE node_grid_cache_cell SET update_time=%s
            WHERE grid_id=%s AND x_index=%s AND y_index=%s AND z_index=%s
            RETURNING id
        ''', [started_at, *coordinates])
        receipt['present'] = cursor.fetchone() is not None
        if invalidation is not None:
            cursor.execute('''
                DELETE FROM dirty_node_grid_cache_cell
                WHERE id=%s AND xmin::text=%s
            ''', list(invalidation))
