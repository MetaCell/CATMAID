"""Optional deployment transport for node identities, not recovery checkpoints."""

from __future__ import annotations

import json
import os
import subprocess
from .errors import RetryableError, OperatorAttentionError


def publish_node_map(state_dir, batch_index, *, committed=False):
    raw = os.environ.get("CATMAID_BATCH_IMPORT_NODE_MAP_COMMAND")
    if not raw:
        return
    try:
        command = json.loads(raw)
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(s, str) and s for s in command)
        ):
            raise ValueError("invalid node-map command")
        subprocess.run(
            [
                *command,
                "committed" if committed else "stage",
                str(state_dir),
                str(batch_index),
            ],
            check=True,
            timeout=600,
        )
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        if committed:
            raise OperatorAttentionError(
                "Batch committed but node-map confirmation failed; do not reimport"
            ) from exc
        raise RetryableError(
            "Node-map storage failed before the batch transaction"
        ) from exc
