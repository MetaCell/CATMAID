from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import OperationalError, transaction


# The base lock is formed from the multiplication of all characters of "catmaid"
# as ASCII: 99 * 97 * 116 * 109 * 97 * 105 * 100.
base_lock_id = 123666608142000

# Postgres advisory lock ID to update spatial update even handling
spatial_update_event_lock = base_lock_id + 1
# Postgres advisory lock ID to update history update even handling
history_update_event_lock = base_lock_id + 2


class NeuronLockTimeout(ValueError):
    """The deadline for acquiring a neuron lock expired."""


def lock_skeleton_neurons(project_id, neuron_ids, cursor) -> set[int]:
    """Lock the existing neurons in ID order until transaction commit."""
    timeout_ms = settings.SKELETON_EDIT_LOCK_TIMEOUT_MS
    if (isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int)
            or not 0 < timeout_ms <= 2147483647):
        raise ImproperlyConfigured('SKELETON_EDIT_LOCK_TIMEOUT_MS must be a positive integer')
    try:
        # A savepoint restores the timeout and releases partial locks on failure.
        with transaction.atomic(using=cursor.db.alias):
            cursor.execute("SELECT setting::integer FROM pg_settings WHERE name='lock_timeout'")
            previous_timeout_ms = cursor.fetchone()[0]
            effective_timeout_ms = (min(timeout_ms, previous_timeout_ms)
                    if previous_timeout_ms else timeout_ms)
            cursor.execute("SELECT set_config('lock_timeout', %s, true)",
                    [f'{effective_timeout_ms}ms'])
            cursor.execute("""
                SELECT ci.id
                FROM class_instance ci
                JOIN class c ON c.id = ci.class_id
                WHERE ci.project_id = %s AND ci.id = ANY(%s::bigint[])
                    AND c.class_name = 'neuron'
                ORDER BY ci.id
                FOR NO KEY UPDATE OF ci
            """, (project_id, sorted(neuron_ids)))
            locked = {row[0] for row in cursor.fetchall()}
            cursor.execute("SELECT set_config('lock_timeout', %s, true)",
                    [f'{previous_timeout_ms}ms'])
    except OperationalError as error:
        if getattr(error.__cause__, 'pgcode', None) == '55P03':
            raise NeuronLockTimeout('A source neuron is being edited; retry the operation.') from error
        raise
    return locked
