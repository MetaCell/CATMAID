"""Coordinate concurrent grid cell refreshes and publish each cell atomically."""

from contextlib import contextmanager
import hashlib


def cell_lock_id(grid_id, x, y, z):
    """Give cooperating writers a stable transaction advisory lock per cell."""
    value = f'catmaid.grid-cell:{grid_id}:{x}:{y}:{z}'.encode('ascii')
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'big', signed=True)


@contextmanager
def locked_grid_cell(grid_id, x, y, z, *, connection=None,
        atomic=None, lock_timeout_ms=None, statement_timeout_ms=None):
    """Publish all formats atomically and acknowledge only an observed invalidation.

    Spatial notifications may arrive while a cell is being generated. Compare
    the dirty row's MVCC version, not just its timestamp (``now()`` is the
    invalidator's transaction start time), so a newer invalidation survives.

    Cache workers and other refresh callers must acquire the lock before reading
    source data. This prevents an older calculation from overwriting a newer
    result. Readers see the previous committed payload until every format and
    the invalidation acknowledgement commit together. Writers that do not use
    this helper do not participate in its advisory locking protocol.
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
