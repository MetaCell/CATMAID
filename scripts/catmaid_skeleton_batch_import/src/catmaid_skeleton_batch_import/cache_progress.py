"""Optional, bounded cache observations; never a recovery checkpoint."""
import time

from .util import atomic_write_json


class CacheProgressWriter:
    def __init__(self, state_dir, *, clock=time.monotonic):
        self.path = state_dir / "cache-progress.json"
        self.clock = clock
        self.last_write = float("-inf")

    def __call__(self, completed, total):
        now = self.clock()
        if completed not in (0, total) and now - self.last_write < 1:
            return
        self.last_write = now
        try:
            atomic_write_json(self.path, {
                "schema_version": 1, "completed": completed, "total": total,
            })
        except OSError:
            # Losing an observation must not stop a verified cache operation.
            pass
