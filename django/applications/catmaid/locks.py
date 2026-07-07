# The base lock is formed from the multiplication of all characters of "catmaid"
# as ASCII: 99 * 97 * 116 * 109 * 97 * 105 * 100.
base_lock_id = 123666608142000

# Postgres advisory lock ID to update spatial update even handling
spatial_update_event_lock = base_lock_id + 1
# Postgres advisory lock ID to update history update even handling
history_update_event_lock = base_lock_id + 2


def _signed_int32(value):
    value = int(value) & 0xffffffff
    if value >= 2 ** 31:
        return value - 2 ** 32
    return value


# Postgres advisory lock namespace for historic skeleton restores. This is
# used with pg_advisory_xact_lock(integer, integer), which is separate from the
# one-key bigint advisory lock space used by the global locks above.
skeleton_restore_lock_namespace = _signed_int32(base_lock_id + 3)


def skeleton_restore_lock_keys(skeleton_id):
    """Return stable signed 32-bit advisory lock keys for a skeleton restore."""
    return skeleton_restore_lock_namespace, _signed_int32(skeleton_id)
