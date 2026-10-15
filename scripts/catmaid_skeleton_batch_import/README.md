# CATMAID skeleton batch importer

This package provides non-interactive CATMAID SWC batch ingestion. It owns
validation, deterministic
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

## Source fragment identifiers

Every SWC creates one protected `source-fragment:<fragment_id>` annotation on
its modeled neuron. The identifier is the exact archive-member basename with
only the final `.swc` extension removed, case-insensitively. For example,
`191805/0021814.swc` yields `0021814`, and `1054599727.23.SWC` yields
`1054599727.23`. Values remain strings: leading zeros, case, dots, whitespace,
Unicode and template-looking text such as `{n}` are not transformed.

Planning rejects empty identifiers, duplicate identifiers anywhere in the
archive, and identifiers longer than 239 characters. The last limit leaves room
for the 16-character prefix in CATMAID's 255-character annotation field. It is
per identifier, not a combined limit on the number or total length of a
segment's contributing identifiers. Visible neuron and skeleton names keep
CATMAID's normal behavior and are not the source of truth for these values.

Merges retain the union of both source-identifier sets, including with an empty
client annotation selection. Splits copy the entire set to both results,
including when a client requests no ordinary annotations on the new half.
Duplicates are not linked twice. This records inherited provenance; after a
split it does not assert that every original fragment still supplies nodes to
both results.

The namespace is importer-managed. Manual creation, attachment, rename,
replacement and removal through CATMAID editing APIs return HTTP 403, including
for ordinary superuser editing requests. Trusted structural copies and merges
preserve literal identifiers. Native SWC annotation replacement keeps existing
source identifiers, but native SWC uploads do not start generating them. Whole
neuron, skeleton and project deletion still work normally.

The source-to-CATMAID manifest includes `fragment_id` independently of
`external_skeleton_id` and display `name`. Annotation rows and links commit or
roll back in the same atomic batch as morphology, and final verification checks
the exact values and ownership.

### Search and audit API contract

Use the existing project-authorized CATMAID APIs in two steps. First resolve an
exact annotation name to its annotation ID:

```text
POST /{project_id}/annotations/query-targets
types[0]=annotation
name=source-fragment:0021814
name_exact=true
name_case_sensitive=true
with_annotations=false
```

Form-encode the name as a literal string. If no annotation is found, return no
matches; do not issue an unfiltered second query. Then find the currently
associated neurons and skeletons using the numeric annotation ID:

```text
POST /{project_id}/annotations/query-targets
types[0]=neuron
annotation_reference=id
annotated_with=<annotation_id>
with_annotations=true
```

The response's entities include the current neuron `name` and `skeleton_ids`.
Using annotation IDs for the second call avoids comma-delimited name parsing.
For a batch audit, `POST /{project_id}/annotations/forskeletons` accepts
`skeleton_ids[0]`, etc., and returns skeleton annotation links plus an
annotation-ID-to-name dictionary. Filter the exact `source-fragment:` prefix
and remove it once to recover each complete identifier string.

A later Neuroglancer integration can route fragment searches through these
calls and refresh source metadata after merges and splits. Keep fragment IDs
as strings separately from numeric CATMAID skeleton IDs. This change does not
add viewer code or append all identifiers to visible labels.

### Development workspace format

Version 0.3.0 uses new internal plan/state formats and five reserved concept IDs
per fragment: neuron, skeleton, model link, annotation and annotation link.
Older development workspaces are rejected; create a new workspace instead of
rewriting existing state. No legacy reader or backfill is provided. Public
request and wrapper-result schemas remain version 1.

## Container ingestion entry point

The CATMAID image includes the package and a headless runtime wrapper:

```bash
SKELETON_INGESTION_WORKSPACE=/staging \
/home/env/bin/python /home/scripts/docker/run_catmaid_skeleton_import.py plan

SKELETON_INGESTION_WORKSPACE=/staging \
/home/env/bin/python /home/scripts/docker/run_catmaid_skeleton_import.py run
```

The existing absolute workspace must contain `request.json`; the wrapper creates
or resumes its `state` directory. `plan` needs no database. `run` requires
`DB_HOST`, `DB_NAME`, `DB_USER` and `DB_PASS`, with optional `DB_PORT` (default
5432), and uses `mysite.ingestion_settings` without starting the web server or
applying migrations. `CATMAID_INGESTION_DB_WAIT_SECONDS` controls the bounded
readiness wait (default 120 seconds, maximum 3600); the wrapper does not retry
failed ingestion automatically.

`CATMAID_PROJECTS_DIRECTORY` defaults to `/home/django/projects`, and
`CATMAID_INGESTION_MEDIA_ROOT` defaults to
`/tmp/catmaid-skeleton-ingestion-media`. A failed child command records its result
in `state/run-wrapper-result.json`. Input downloads, workflow submission,
publication and failure cleanup remain the caller's responsibility.

## Deployment settings

The importer reads a Django setting first, then the same-name environment
variable, then its module default:

| Setting | Default |
|---|---:|
| `CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH` | `250000` |
| `CATMAID_BATCH_IMPORT_MAX_ARCHIVE_BYTES` | `2147483648` (2 GiB) |
| `CATMAID_BATCH_IMPORT_MAX_SWC_BYTES` | `2147483648` (2 GiB) |
| `CATMAID_BATCH_IMPORT_MAX_SWC_COUNT` | `250000` |
| `CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES` | disabled |
| `CATMAID_BATCH_IMPORT_CACHE_CELL_SIZE_NM` | `1500000` |
| `CATMAID_BATCH_IMPORT_LOD_LEVELS` | `1` |
| `CATMAID_BATCH_IMPORT_LOD_BUCKET_SIZE` | `500` |
| `CATMAID_BATCH_IMPORT_LOD_STRATEGY` | `quadratic` |
| `CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS` | `0` |
| `CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS` | `30000` |

The default cache is one 1.5 mm XY MessagePack grid with a single LOD bucket.
The viewer requests the sole spatial profile at 100% LOD, so the bucket contains
all entries without progressive subdivision. Bucket size and strategy remain
configurable for explicit multi-level overrides.

Planning snapshots all result-affecting settings. Existing plans retain their
recorded LOD count when resumed or rebuilt. Database timeouts are resolved
again for each `run` and recorded with its metrics.

## Failure and resume contract

Each skeleton stays inside one database batch. Core rows, the `model_of`
relationship, fragment annotations and links, direct batch edge creation,
canonical summary refresh, and validation commit
or roll back together. A confirmed rollback is safe to retry with `run`, and
earlier committed batches remain untouched.

The state is durably marked immediately before each COMMIT attempt. If the
process cannot prove the outcome, the ingestion becomes `operator_attention`.
Normal `run` calls then refuse every CATMAID mutation until an operator resolves
the uncertainty. The importer does not guess, automatically reconcile, or
automatically delete the project.

The project remains hidden after successful verification. Publication is a
separate caller/operator responsibility.

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

The opt-in PostgreSQL tests create hidden projects, inject failures, verify exact
edge geometry and summary parity, and resume committed batches. Use only a
disposable, migrated CATMAID/PostGIS database; the tests leave their hidden
projects for inspection. Name that database and pin its PostgreSQL cluster
identity explicitly before enabling either database test module:

```bash
CATMAID_BATCH_IMPORT_E2E=1 \
CATMAID_BATCH_IMPORT_LOCAL_TEST=isolated \
CATMAID_BATCH_IMPORT_TEST_DB_HOST=catmaid-test-db \
CATMAID_BATCH_IMPORT_TEST_DB_NAME=catmaid_test \
CATMAID_BATCH_IMPORT_TEST_DB_SYSTEM_IDENTIFIER='<disposable cluster identifier>' \
DJANGO_SETTINGS_MODULE=mysite.settings \
python -m pytest -q \
  scripts/catmaid_skeleton_batch_import/tests/test_e2e_postgis.py \
  scripts/catmaid_skeleton_batch_import/tests/test_edges_postgis.py
```

The configured Django connection must match the named host and database. Obtain
the cluster identifier from that disposable database with
`SELECT system_identifier::text FROM pg_control_system()`; no production target
is a suitable substitute. These checks run before the tests create any projects.

## Edge materialization

The edge materialization introduced in version 0.2.0 creates edges from a validated temporary copy of each prepared
batch, using PostgreSQL `real` coordinates to match CATMAID's stored precision.
It checks recorded IDs, ownership, and parent membership before inserting one
edge per node. Roots retain NULL parents and zero-length geometry. Existing
edges cause an error and batch rollback; no edges or connector materializations
are deleted or rebuilt.

Core rows, edges, the unchanged CATMAID summary refresh, and validation remain
inside one atomic transaction. Current-format state files keep their immutable
settings and recorded IDs. Committed batches are skipped on resume, while
uncertain commit outcomes continue to block further writes.

Metrics record `edge_staging`, `edge_creation`, and `summary_refresh` within the
existing `materialization` aggregate, plus the executing importer version for
each run. Historical metrics remain readable. Do not add these child timings
to `materialization`, because they overlap.

Paired local benchmarks on a populated 42.5-million-node database measured
2.78x faster edge work, including staging, and 53.9% lower batch transaction
time when comparing sums of per-fixture medians. The 113-test correctness run
and full 171-batch rehearsal passed. These are local measurements and do not
establish a cluster speedup.
