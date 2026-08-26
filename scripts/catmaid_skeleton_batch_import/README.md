# CATMAID skeleton batch importer

This package is the supported, non-interactive replacement for the Bash-based
SWC bulk-import workflow in this directory. It owns validation, deterministic
batch planning, database ingestion, CATMAID materialization, cache creation,
verification, and durable resume state. It does not download inputs, publish a
project, or delete a failed project.

## Interface

Install the package in the same Python environment as CATMAID:

```bash
python -m pip install ./scripts/catmaid_skeleton_batch_import
```

Then prepare and run one ingestion:

```bash
catmaid-skeleton-import plan \
  --request /staging/request.json \
  --state-dir /staging/state

DJANGO_SETTINGS_MODULE=mysite.settings \
catmaid-skeleton-import run --state-dir /staging/state
```

After a successful import, `run` skips ingestion and cache creation but repeats
bounded read-only verification. To cleanly rebuild the one importer-owned cache
and then verify it again, use:

```bash
catmaid-skeleton-import run \
  --state-dir /staging/state \
  --rebuild-cache
```

The rebuild first invalidates the cache and verification checkpoints. If it is
interrupted, an ordinary later `run` resumes at cache creation without changing
skeleton rows.

`plan` never mutates the database. `run` reads only the durable state directory
and the unchanged local input files recorded there. Run the command from a
CATMAID application environment where `mysite.settings` and the `catmaid`
package are importable.

The request schema is deliberately small:

```json
{
  "schema_version": 1,
  "source": {
    "archive_path": "/staging/191805.zip",
    "coordinate_unit": "um"
  },
  "stack": {
    "dimension": [17797, 30801, 10826],
    "resolution_nm": [350, 350, 1000]
  }
}
```

`source.external_skeleton_id_map_path` may name a local CSV with exactly the
columns `member_path,external_skeleton_id`. The mapping enriches the output
manifest but never chooses CATMAID IDs.

Coordinates and non-sentinel radii use the same declared physical unit. An SWC
radius of `-1` remains CATMAID's unknown-radius sentinel. Voxel and anisotropic
source coordinates are not supported.

## Deployment settings

The importer reads a Django setting first, then the same-name environment
variable, then its module default:

| Setting | Default |
|---|---:|
| `CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH` | `250000` |
| `CATMAID_BATCH_IMPORT_MAX_ARCHIVE_BYTES` | `10737418240` (10 GiB) |
| `CATMAID_BATCH_IMPORT_MAX_SWC_BYTES` | `2147483648` (2 GiB) |
| `CATMAID_BATCH_IMPORT_MAX_SWC_COUNT` | `250000` |
| `CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES` | disabled |
| `CATMAID_BATCH_IMPORT_CACHE_CELL_SIZE_NM` | `1500000` |
| `CATMAID_BATCH_IMPORT_LOD_LEVELS` | `7` |
| `CATMAID_BATCH_IMPORT_LOD_BUCKET_SIZE` | `500` |
| `CATMAID_BATCH_IMPORT_LOD_STRATEGY` | `quadratic` |
| `CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS` | `0` |
| `CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS` | `30000` |

Planning snapshots all result-affecting settings. Database timeouts are resolved
again for each `run` and recorded with its metrics.

## Failure and resume contract

Each skeleton stays inside one database batch. Core rows, the `model_of`
relationship, selective edge and summary materialization, and validation commit
or roll back together. A confirmed rollback is safe to retry with `run`, and
earlier committed batches remain untouched.

The state is durably marked immediately before each COMMIT attempt. If the
process cannot prove the outcome, the ingestion becomes `operator_attention`.
Normal `run` calls then refuse every CATMAID mutation until an operator resolves
the uncertainty. The importer does not guess, automatically reconcile, or
automatically delete the project.

The project remains hidden after successful verification. Publication is a
separate MNP/operator responsibility.

## Exit codes

| Code | Meaning |
|---:|---|
| `0` | Command completed successfully |
| `2` | Invalid request, archive, mapping, setting, or deployment precondition |
| `3` | Definite failure that is safe to retry with `run` |
| `4` | Immutable request/archive/plan/database target mismatch |
| `5` | Uncertain outcome requiring operator attention |
| `6` | Final read-only verification failed |

The command writes one versioned JSON result to stdout and progress diagnostics
to stderr.

## Tests

Run the dependency-free suite with:

```bash
python -m pytest -q scripts/catmaid_skeleton_batch_import/tests
```

An opt-in PostgreSQL test injects a second-batch failure, proves that the first
batch remains committed, and resumes the same project. It intentionally leaves
the resulting hidden test project in the configured database for inspection:

```bash
CATMAID_BATCH_IMPORT_E2E=1 \
DJANGO_SETTINGS_MODULE=mysite.settings \
python -m pytest -q \
  scripts/catmaid_skeleton_batch_import/tests/test_e2e_postgis.py
```

## Legacy scripts

The existing shell scripts remain temporarily as migration references. They are
not part of the new CLI and should be removed only after representative imports
have passed parity and operational acceptance.
