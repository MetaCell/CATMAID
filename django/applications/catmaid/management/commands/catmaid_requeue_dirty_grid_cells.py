import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from .common import set_log_level


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Replay persisted dirty grid cache rows once on startup by touching "
        "their invalidation time so CATMAID emits fresh dirty-cache NOTIFY "
        "events."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--startup-delay',
            type=float,
            default=10,
            help='Seconds to wait before replaying dirty grid cells.',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=10,
            help='Maximum number of dirty rows to replay per batch.',
        )

    def handle(self, *args, **options):
        set_log_level(logger, options.get('verbosity', 1))

        startup_delay = options['startup_delay']
        batch_size = options['batch_size']

        if startup_delay < 0:
            raise CommandError('--startup-delay must be greater than or equal to 0')
        if batch_size <= 0:
            raise CommandError('--batch-size must be greater than 0')

        replay_cutoff = self._get_replay_cutoff()
        logger.info(
            'Captured startup dirty-cell replay cutoff at %s',
            replay_cutoff.isoformat(),
        )

        if startup_delay:
            logger.info(
                'Waiting %.1f seconds before replaying dirty CATMAID grid cells',
                startup_delay,
            )
            time.sleep(startup_delay)

        total_rows_touched = 0
        batch_count = 0

        while True:
            rows_touched, oldest_dirty_age_seconds = self._requeue_batch(
                batch_size,
                replay_cutoff,
            )
            if rows_touched == 0:
                break

            batch_count += 1
            total_rows_touched += rows_touched
            logger.info(
                'Replayed batch %s of dirty CATMAID grid cells: rows_touched=%s oldest_dirty_age_seconds=%.3f',
                batch_count,
                rows_touched,
                oldest_dirty_age_seconds,
            )

        logger.info(
            'Startup dirty-cell replay finished: rows_touched=%s batch_count=%s replay_cutoff=%s',
            total_rows_touched,
            batch_count,
            replay_cutoff.isoformat(),
        )

    def _get_replay_cutoff(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT statement_timestamp()")
            row = cursor.fetchone()

        return row[0]

    def _requeue_batch(self, batch_size, replay_cutoff):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH target AS (
                    SELECT
                        dirty.id,
                        EXTRACT(
                            EPOCH FROM (statement_timestamp() - dirty.invalidation_time)
                        ) AS age_seconds
                    FROM dirty_node_grid_cache_cell AS dirty
                    INNER JOIN node_grid_cache AS grid
                        ON grid.id = dirty.grid_id
                    WHERE grid.enabled = TRUE
                      AND dirty.invalidation_time <= %s
                    ORDER BY dirty.invalidation_time ASC, dirty.id ASC
                    LIMIT %s
                ),
                updated AS (
                    UPDATE dirty_node_grid_cache_cell AS dirty
                    SET invalidation_time = statement_timestamp()
                    FROM target
                    WHERE dirty.id = target.id
                    RETURNING dirty.id
                )
                SELECT
                    COALESCE((SELECT COUNT(*) FROM updated), 0) AS rows_touched,
                    COALESCE((SELECT MAX(age_seconds) FROM target), 0) AS oldest_dirty_age_seconds
                """,
                [replay_cutoff, batch_size],
            )
            row = cursor.fetchone()

        return int(row[0]), float(row[1])
