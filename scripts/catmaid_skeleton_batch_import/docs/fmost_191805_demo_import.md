# fMOST 191805 demo CATMAID import runbook

This runbook imports the fMOST 191805 autotrace skeletons into a CATMAID demo
cluster project named `Allen fMost`.

## Strategy

Use dev as a full rehearsal, then repeat the same no-downtime import directly
on demo. Do not export the populated project from dev and import it into demo;
that adds a large database movement step and demo still needs its own derived
table and grid cache rebuilds.

The import keeps CATMAID online. The new project is created hidden from normal
users, the SWCs are loaded with explicit IDs reserved from PostgreSQL sequences,
and template permissions are copied only after materialization, cache rebuilds,
and verification pass.

High-level flow:

1. Select the cluster and set `IMPORT_CONFIG`.
2. Create a hidden `Allen fMost` project/stack from the config.
3. Download `191805.zip` to the local path referenced by the config.
4. Run the online bulk importer.
5. Rebuild `treenode_edge`, skeleton summaries, project stats, and grid caches.
6. Verify counts/bounds and marker mapping.
7. Publish by copying permissions from the template project.

## 1. Source facts

Public S3 source:

```text
s3://aind-open-data/fmost_443055-191805/
```

Relevant objects:

```text
s3://aind-open-data/fmost_443055-191805/fused.zarr/
s3://aind-open-data/fmost_443055-191805/autotrace/191805.zip
```

OME-Zarr level 0:

```text
axes: t, c, z, y, x
shape: 1 x 1 x 10826 x 30801 x 17797
scale: 1 ms, 1 channel, 1.0 um z, 0.35 um y, 0.35 um x
```

CATMAID stack:

```text
title: Allen fMost
dimension: 17797 x 30801 x 10826 pixels
resolution: 350 x 350 x 1000 nm
orientation: XY
translation: 0, 0, 0
physical extent:
  x: 6,228,950 nm
  y: 10,780,350 nm
  z: 10,826,000 nm
```

SWC archive scan:

```text
SWC files: 129,302
treenodes: 1,469,021
SWC coordinate units: micrometers
SWC bounds:
  x: 672.0 .. 5531.05 um
  y: 1391.3 .. 9471.35 um
  z: 1485.369 .. 10825.45 um
radius bounds:
  r: 0.35 .. 3.882 um
```

The import config encodes the required micrometer-to-nanometer scaling:

```bash
export IMPORT_TOOLS="$PWD/scripts/catmaid_skeleton_batch_import"
export IMPORT_CONFIG="$IMPORT_TOOLS/configs/fmost_191805.json"
```

## 2. Set cluster values

Run the full flow in dev first.

```bash
kubectl config use-context <dev-context>

export NS=<dev-namespace>
export CATMAID_URL=https://<dev-catmaid-host>

export DB_USER=<db-user>
export DB_NAME=<db-name>
export DB_PASS=<db-password>

export IMPORT_TOOLS="$PWD/scripts/catmaid_skeleton_batch_import"
export IMPORT_CONFIG="$IMPORT_TOOLS/configs/fmost_191805.json"

export DB_POD="$(kubectl -n "$NS" get pod -l app=catmaid-db -o jsonpath='{.items[0].metadata.name}')"
export APP_POD="$(kubectl -n "$NS" get pod -l app=catmaid -o jsonpath='{.items[0].metadata.name}')"
```

Verify the selected cluster:

```bash
kubectl config current-context
kubectl -n "$NS" get pod "$APP_POD" "$DB_POD"
kubectl -n "$NS" exec "$DB_POD" -- pg_isready -h localhost -U "$DB_USER" -d "$DB_NAME"
```

## 3. Identify the permission template

The template project must exist in the target cluster database. Set it before
publishing:

```bash
export TEMPLATE_PROJECT_ID=<target-cluster-template-project-id>
```

The import does not copy template permissions during project creation. The
project stays hidden until section 12.

## 4. Create the hidden project and stack

```bash
eval "$("$IMPORT_TOOLS/create_project.sh")"
```

Expected output:

```bash
export PROJECT_ID=<created-or-existing-project-id>
export STACK_ID=<created-or-existing-stack-id>
export IMPORT_USER_ID=<system-user-id>
```

The create step clears object permissions on the target project and grants only
the CATMAID system user `can_browse`, `can_annotate`, `can_import`, and
`can_administer`.

Verify the project metadata:

```bash
kubectl -n "$NS" exec -i "$APP_POD" -- env PROJECT_ID="$PROJECT_ID" bash -lc \
  'cd /home/django/projects && /home/env/bin/python manage.py shell' <<'PY'
import os
from catmaid.models import Project, ProjectStack, StackMirror, Treenode

def triple(value):
    if all(hasattr(value, attr) for attr in ("x", "y", "z")):
        return f"{value.x},{value.y},{value.z}"
    return ",".join(str(v) for v in value)

project = Project.objects.get(pk=int(os.environ["PROJECT_ID"]))
project_stack = ProjectStack.objects.get(project=project)
stack = project_stack.stack

print(f"PROJECT_ID={project.id}")
print(f"PROJECT_TITLE={project.title}")
print(f"STACK_ID={stack.id}")
print(f"STACK_TITLE={stack.title}")
print(f"STACK_DIMENSION={triple(stack.dimension)}")
print(f"STACK_RESOLUTION={triple(stack.resolution)}")
print(f"STACK_METADATA={stack.metadata}")
print(f"STACK_MIRRORS={StackMirror.objects.filter(stack=stack).count()}")
print(f"TREENODES={Treenode.objects.filter(project=project).count()}")
PY
```

## 5. Download the SWC source

Export the SWC zip path explicitly so the local working directory is not tied
to the sample path in the config.

```bash
export FMOST_WORKDIR=<local-fmost-workdir>
export SWC_ZIP_PATH="$FMOST_WORKDIR/191805.zip"
mkdir -p "$FMOST_WORKDIR"

aws s3 cp \
  s3://aind-open-data/fmost_443055-191805/autotrace/191805.zip \
  "$SWC_ZIP_PATH" \
  --no-sign-request

unzip -Z1 "$SWC_ZIP_PATH" | wc -l
unzip -p "$SWC_ZIP_PATH" 191805/0000001.swc | sed -n '1,10p'
```

Expected:

```text
129302 SWC files
```

## 6. Optional external backup

If demo does not already have a suitable database backup or snapshot, take one
with the deployment's standard backup workflow before loading. The batch import
helpers do not include a backup script; the no-downtime path keeps CATMAID up
and relies on hidden-project staging plus sequence-reserved IDs.

## 7. Bulk import while CATMAID stays up

The runner does not scale the CATMAID deployment down. It scans each batch,
reserves exact `concept` and `location` IDs by consuming PostgreSQL sequence
values into ID list files, generates CSVs from the scan plan, and loads them
with `COPY`.

For fMOST, the source is one zip, so one batch is expected:

```bash
export STAGING_BASE="$FMOST_WORKDIR/catmaid_import_${PROJECT_ID}_batches"
export BATCH_MAX_ZIPS=1

"$IMPORT_TOOLS/run_batched_append.sh"
```

Retain:

```text
$STAGING_BASE/completed/batch_00001/scan_summary.json
$STAGING_BASE/completed/batch_00001/summary.json
$STAGING_BASE/completed/batch_00001/batch_plan.tsv
$STAGING_BASE/completed/batch_00001/reserved_concept_ids.txt.gz
$STAGING_BASE/completed/batch_00001/reserved_location_ids.txt.gz
$STAGING_BASE/completed/batch_00001/manifest.tsv.gz
```

The manifest maps each source SWC member to CATMAID neuron and skeleton IDs.
The direct bulk load intentionally skips API-level side effects such as import
log rows, default `Import` annotations, and provenance rows.
By default, the loader leaves database triggers enabled because the normal
`catmaid_user` role cannot set `session_replication_role`. A privileged DB role
can opt into trigger disabling with `LOAD_DISABLE_TRIGGERS=true`, which also
skips trigger-driven history rows. The post-load rebuild steps below should
still be run either way.

## 8. Rebuild materialized tracing data

For this dataset, target rows should be `1469021`.

The DB pod filesystem is read-only, so do not copy files into `/tmp`. Stream
the SQL into `psql`, then start each long `CALL` in the DB pod without writing
log files.

Set locals:

```bash
export PROJECT_ID=<created-or-existing-project-id>
export BATCH_SIZE=250000
export TARGET_ROWS=1469021
export DB_USER=<db-user>
export DB_NAME=<db-name>
```

Install the edge rebuild procedure by streaming the local SQL file:

```bash
kubectl -n "$NS" exec -i "$DB_POD" -c postgres -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
  -f - < "$IMPORT_TOOLS/rebuild_treenode_edges_batched.sql"
```

Start the edge rebuild remotely:

```bash
kubectl -n "$NS" exec "$DB_POD" -c postgres -- env \
  PGPASSWORD="$DB_PASS" \
  DB_USER="$DB_USER" \
  DB_NAME="$DB_NAME" \
  PROJECT_ID="$PROJECT_ID" \
  BATCH_SIZE="$BATCH_SIZE" \
  TARGET_ROWS="$TARGET_ROWS" \
  PGAPPNAME="fmost_edge_rebuild_project_$PROJECT_ID" \
  sh -c 'nohup psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "CALL public.catmaid_skeleton_rebuild_treenode_edges_batched(${PROJECT_ID}, ${BATCH_SIZE}, ${TARGET_ROWS});" \
    >/proc/1/fd/1 2>/proc/1/fd/2 </dev/null &'
```

Check edge rebuild progress:

```bash
kubectl -n "$NS" exec "$DB_POD" -c postgres -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -x -c "
SELECT *
FROM public.catmaid_skeleton_treenode_edge_rebuild_status
WHERE project_id = $PROJECT_ID;
"
```

After the edge rebuild completes, refresh PostgreSQL planner statistics before
starting the summary/stat refresh. This helps later summary batches avoid stale
plans after the bulk load and `treenode_edge` rebuild:

```bash
kubectl -n "$NS" exec "$DB_POD" -c postgres -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
ANALYZE treenode;
ANALYZE treenode_edge;
"
```

When edge rebuild status is `complete`, install the summary/stat refresh
procedure:

```bash
kubectl -n "$NS" exec -i "$DB_POD" -c postgres -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
  -f - < "$IMPORT_TOOLS/refresh_summary_stats_by_node_batched.sql"
```

Start the summary/stat refresh remotely:

```bash
kubectl -n "$NS" exec "$DB_POD" -c postgres -- env \
  PGPASSWORD="$DB_PASS" \
  DB_USER="$DB_USER" \
  DB_NAME="$DB_NAME" \
  PROJECT_ID="$PROJECT_ID" \
  BATCH_SIZE="$BATCH_SIZE" \
  TARGET_ROWS="$TARGET_ROWS" \
  PGAPPNAME="fmost_summary_refresh_project_$PROJECT_ID" \
  sh -c 'nohup psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "CALL public.catmaid_skeleton_refresh_summary_stats_by_node_batched(${PROJECT_ID}, ${BATCH_SIZE}, ${TARGET_ROWS});" \
    >/proc/1/fd/1 2>/proc/1/fd/2 </dev/null &'
```

Check summary/stat refresh progress:

```bash
kubectl -n "$NS" exec "$DB_POD" -c postgres -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -x -c "
SELECT *
FROM public.catmaid_skeleton_summary_stats_refresh_status
WHERE project_id = $PROJECT_ID;
"
```

## 9. Build grid caches

The grid runner reads fMOST bounds from the config. If the helper directory is
mounted at `/scripts`, use the config path inside that mount. If the config is
not mounted, pass `IMPORT_CONFIG_JSON="$(cat "$IMPORT_CONFIG")"` instead.

```bash
PROJECT_ID="$PROJECT_ID" IMPORT_CONFIG=/scripts/configs/fmost_191805.json \
CELL_SIZE=1500000 SLAB_SIZE=1500000 START_SLAB=0 CLEAN_FIRST=false \
JOBS=2 CHUNK_SIZE=5 LOD_LEVELS=7 LOD_BUCKET_SIZE=500 LOD_STRATEGY=quadratic \
/scripts/run_grid_cache_slabs.sh
```

If running from this checkout against a remote app pod, copy the grid runner
and config into the pod and run the cache build there:

```bash
kubectl -n "$NS" exec "$APP_POD" -- mkdir -p /scripts/configs

kubectl -n "$NS" cp \
  "$IMPORT_TOOLS/run_grid_cache_slabs.sh" \
  "$APP_POD:/scripts/run_grid_cache_slabs.sh"
kubectl -n "$NS" cp \
  "$IMPORT_TOOLS/configs/fmost_191805.json" \
  "$APP_POD:/scripts/configs/fmost_191805.json"

kubectl -n "$NS" exec "$APP_POD" -- env \
  PROJECT_ID="$PROJECT_ID" \
  IMPORT_CONFIG=/scripts/configs/fmost_191805.json \
  CELL_SIZE=1500000 \
  SLAB_SIZE=1500000 \
  START_SLAB=0 \
  CLEAN_FIRST=false \
  JOBS=2 \
  CHUNK_SIZE=5 \
  LOD_LEVELS=7 \
  LOD_BUCKET_SIZE=500 \
  LOD_STRATEGY=quadratic \
  bash /scripts/run_grid_cache_slabs.sh
```

Repeat with any additional cell sizes used by demo. Do not set
`CLEAN_FIRST=true` unless you intentionally want to delete all existing grid
caches for this project/orientation.

## 10. Verify before publish

```bash
"$IMPORT_TOOLS/verify_import.sh"
```

Expected values:

```text
skeletons: 129302
treenodes: 1469021
x bounds: 672000 .. 5531050 nm
y bounds: 1391300 .. 9471350 nm
z bounds: 1485369 .. 10825450 nm
radius bounds: 350 .. 3882 nm
```

Confirm sequences are above imported IDs:

```bash
kubectl -n "$NS" exec "$DB_POD" -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -x -c "
SELECT
  (SELECT last_value FROM concept_id_seq) AS concept_seq_last,
  (SELECT max(id) FROM concept) AS concept_max_id,
  (SELECT last_value FROM location_id_seq) AS location_seq_last,
  (SELECT max(id) FROM location) AS location_max_id;
"
```

The sequence values may be higher than max IDs because gaps are acceptable.
They must not be lower.

## 11. Map soma CSV marker IDs to skeletons

Marker IDs map directly to SWC member names by zero-padding the marker number:

```text
191805_markerID-1554 -> 191805/0001554.swc
191805_markerID-1    -> 191805/0000001.swc
```

Build a marker-to-skeleton map from the manifest:

```bash
export SOMA_CSV=/path/to/fmost_soma_markers.csv
export MANIFEST="$STAGING_BASE/completed/batch_00001/manifest.tsv.gz"
export MARKER_MAP="$FMOST_WORKDIR/soma_marker_skeleton_map.tsv"

python3 - <<'PY'
import ast
import csv
import gzip
import os
import re

soma_csv = os.environ["SOMA_CSV"]
manifest_path = os.environ["MANIFEST"]
out_path = os.environ["MARKER_MAP"]

member_to_ids = {}
with gzip.open(manifest_path, "rt", newline="") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        member_to_ids[row["member"]] = row

with open(soma_csv, newline="") as in_file, open(out_path, "w", newline="") as out_file:
    reader = csv.DictReader(in_file)
    fieldnames = [
        "marker_id",
        "member",
        "neuron_id",
        "skeleton_id",
        "soma_x_nm",
        "soma_y_nm",
        "soma_z_nm",
    ]
    writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()

    for row in reader:
        marker_id = row["brainID-markerID"]
        match = re.search(r"markerID-(\d+)$", marker_id)
        if not match:
            raise ValueError(f"Cannot parse marker ID: {marker_id}")

        member = f"191805/{int(match.group(1)):07d}.swc"
        if member not in member_to_ids:
            raise ValueError(f"No imported SWC found for marker {marker_id}: {member}")

        x_um, y_um, z_um = ast.literal_eval(row["raw_um_xyz_coordinate"])
        imported = member_to_ids[member]
        writer.writerow({
            "marker_id": marker_id,
            "member": member,
            "neuron_id": imported["neuron_id"],
            "skeleton_id": imported["skeleton_id"],
            "soma_x_nm": f"{x_um * 1000:.3f}",
            "soma_y_nm": f"{y_um * 1000:.3f}",
            "soma_z_nm": f"{z_um * 1000:.3f}",
        })

print(out_path)
PY

sed -n '1,20p' "$MARKER_MAP"
```

## 12. Publish permissions

Only publish after verification passes:

```bash
"$IMPORT_TOOLS/publish_project_permissions.sh"
```

This clears current object permissions, copies all user/group object
permissions from `TEMPLATE_PROJECT_ID`, and re-adds system-user admin/import
permissions.

## 13. Annotate skeletons with soma marker IDs

For a small marker CSV, use the CATMAID annotation API after publishing.
CATMAID's `annotations/add` endpoint accepts `skeleton_ids[...]` and resolves
them to the modeled neurons.

```bash
export CATMAID_USER=<dev-user>
read -rsp "CATMAID password: " CATMAID_PASSWORD
printf '\n'

export CATMAID_TOKEN="$(
  curl -fsS -X POST "$CATMAID_URL/api-token-auth/" \
    --data-urlencode "username=$CATMAID_USER" \
    --data-urlencode "password=$CATMAID_PASSWORD" |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
)"
unset CATMAID_PASSWORD
```

Annotate each mapped skeleton:

```bash
tail -n +2 "$MARKER_MAP" |
  while IFS=$'\t' read -r marker_id member neuron_id skeleton_id soma_x_nm soma_y_nm soma_z_nm; do
    curl -fsS \
      -H "X-Authorization: Token $CATMAID_TOKEN" \
      -F "skeleton_ids[0]=${skeleton_id}" \
      -F "annotations[0]=${marker_id}" \
      -F "annotations[1]=autodetected soma" \
      -F "annotations[2]=fMOST 191805" \
      "$CATMAID_URL/$PROJECT_ID/annotations/add" \
      > /dev/null

    printf 'Annotated skeleton %s with %s\n' "$skeleton_id" "$marker_id"
  done
```

Keep `$MARKER_MAP` as the authoritative mapping for marker coordinates.

## 14. Handoff checklist

- The demo project opens in CATMAID after publishing.
- Skeleton count is `129302`.
- Treenode count is `1469021`.
- `catmaid_skeleton_summary` and `catmaid_stats_summary` are populated.
- Grid caches exist and have no dirty cells for this project.
- Marker annotation spot checks work by searching for a marker ID, for example
  `191805_markerID-1554`.
- The database backup and `$STAGING_BASE/completed` manifests are retained.

## Rollback

If the load fails before publishing, delete the hidden project and its data.
This does not restore database sequences; sequence gaps are expected and safe.

If the load partially succeeds and the database state is uncertain, restore the
pre-import database backup created in section 6.
