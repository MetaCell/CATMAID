from contextlib import contextmanager

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import OperationalError, transaction

from catmaid.error import ClientError


# The base lock is formed from the multiplication of all characters of "catmaid"
# as ASCII: 99 * 97 * 116 * 109 * 97 * 105 * 100.
base_lock_id = 123666608142000

# Postgres advisory lock ID to update spatial update even handling
spatial_update_event_lock = base_lock_id + 1
# Postgres advisory lock ID to update history update even handling
history_update_event_lock = base_lock_id + 2


class SkeletonEditLockTimeout(ClientError):
    """The wait for a skeleton edit lock timed out."""

    status_code = 409
    meta = {"code": "edit_lock_timeout"}


@contextmanager
def skeleton_edit_lock_timeout(cursor):
    """Bound lock acquisition while holding acquired locks until transaction commit."""
    timeout_ms = settings.SKELETON_EDIT_LOCK_TIMEOUT_MS
    if (isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int)
            or not 0 < timeout_ms <= 2147483647):
        raise ImproperlyConfigured('SKELETON_EDIT_LOCK_TIMEOUT_MS must be a positive integer')

    try:
        # Rolling back the savepoint restores lock_timeout and releases partial locks.
        with transaction.atomic(using=cursor.db.alias):
            cursor.execute("SELECT setting::integer FROM pg_settings WHERE name='lock_timeout'")
            previous_timeout_ms = cursor.fetchone()[0]
            effective_timeout_ms = (min(timeout_ms, previous_timeout_ms)
                    if previous_timeout_ms else timeout_ms)
            cursor.execute("SELECT set_config('lock_timeout', %s, true)",
                    [f'{effective_timeout_ms}ms'])
            yield
            cursor.execute("SELECT set_config('lock_timeout', %s, true)",
                    [f'{previous_timeout_ms}ms'])
    except OperationalError as error:
        cause = error.__cause__
        if (getattr(cause, 'sqlstate', None) or getattr(cause, 'pgcode', None)) == '55P03':
            raise SkeletonEditLockTimeout("The target is being edited. Please retry.") from error
        raise
