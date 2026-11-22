import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from catmaid.control.grid_cache import DEFAULT_REPLAY_BATCH_SIZE, requeue_dirty_grid_cells

from .common import set_log_level


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Send fresh dirty-cache notifications for persisted dirty grid cells."

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
            default=DEFAULT_REPLAY_BATCH_SIZE,
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

        if startup_delay:
            logger.info(
                'Waiting %.1f seconds before replaying dirty CATMAID grid cells',
                startup_delay,
            )
            time.sleep(startup_delay)

        total_cells_notified = 0
        batch_count = 0

        with connection.cursor() as cursor:
            for cells_notified, oldest_dirty_age_seconds in requeue_dirty_grid_cells(cursor, batch_size):
                batch_count += 1
                total_cells_notified += cells_notified
                logger.info(
                    'Replayed batch %s of dirty CATMAID grid cells: cells_notified=%s oldest_dirty_age_seconds=%.3f',
                    batch_count,
                    cells_notified,
                    oldest_dirty_age_seconds,
                )

        logger.info(
            'Dirty-cell replay finished: cells_notified=%s batch_count=%s',
            total_cells_notified,
            batch_count,
        )
