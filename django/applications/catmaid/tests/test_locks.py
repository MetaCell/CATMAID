import unittest

from catmaid import locks


class LocksTest(unittest.TestCase):

    def test_skeleton_restore_lock_keys_are_stable_per_skeleton(self):
        self.assertEqual(
                locks.skeleton_restore_lock_keys(123),
                locks.skeleton_restore_lock_keys(123))
        self.assertEqual(
                locks.skeleton_restore_lock_keys(123),
                locks.skeleton_restore_lock_keys('123'))

    def test_skeleton_restore_lock_resource_key_differs_between_skeletons(self):
        namespace_a, skeleton_key_a = locks.skeleton_restore_lock_keys(123)
        namespace_b, skeleton_key_b = locks.skeleton_restore_lock_keys(124)

        self.assertEqual(namespace_a, namespace_b)
        self.assertNotEqual(skeleton_key_a, skeleton_key_b)

    def test_skeleton_restore_lock_keys_fit_postgresql_integer(self):
        lock_keys = locks.skeleton_restore_lock_keys(123)

        for lock_key in lock_keys:
            self.assertGreaterEqual(lock_key, -(2 ** 31))
            self.assertLessEqual(lock_key, 2 ** 31 - 1)

    def test_skeleton_restore_lock_resource_key_rolls_over_to_int32(self):
        # Different skeleton IDs can fold to the same 32-bit resource key. This
        # only causes false contention between unrelated restores; the same
        # skeleton ID still always maps to the same key and remains serialized.
        self.assertEqual(
                locks.skeleton_restore_lock_keys(1)[1],
                locks.skeleton_restore_lock_keys(2 ** 32 + 1)[1])
