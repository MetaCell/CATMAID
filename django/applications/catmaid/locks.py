import hashlib


# The base lock is formed from the multiplication of all characters of "catmaid"
# as ASCII: 99 * 97 * 116 * 109 * 97 * 105 * 100.
base_lock_id = 123666608142000

# Postgres advisory lock ID to update spatial update even handling
spatial_update_event_lock = base_lock_id + 1
# Postgres advisory lock ID to update history update even handling
history_update_event_lock = base_lock_id + 2
# Postgres advisory lock namespace for historic skeleton restores
skeleton_restore_lock_namespace = base_lock_id + 3


def skeleton_restore_lock_id(skeleton_id):
    """Return a stable signed 64-bit advisory lock ID for a skeleton restore."""
    lock_key = f'{skeleton_restore_lock_namespace}:{int(skeleton_id)}'.encode(
            'ascii')
    unsigned_lock_id = int.from_bytes(
            hashlib.blake2b(lock_key, digest_size=8).digest(), 'big')
    if unsigned_lock_id >= 2 ** 63:
        return unsigned_lock_id - 2 ** 64
    return unsigned_lock_id
