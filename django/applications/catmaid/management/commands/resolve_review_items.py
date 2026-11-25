"""Place NeuroGlass review items on the project's treenodes by position, and label them (NGLASS-2386).

Each review item is one or two points in project space (nm). A point is placed on the single
treenode within the margin on every axis; the margin absorbs the rounding of locations CATMAID
stores as 32-bit floats. Rows that cannot be placed are skipped with a reason. The found nodes get
the review-item label in the same transaction, so no item can exist whose node lacks it.
"""
import gzip
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from catmaid.models import User
from catmaid.review_items import label_treenodes

BATCH = 1_000
MAX_REPORTED = 100
MERGE_ERROR, SPLIT_ERROR = "merge_error", "split_error"

# The GiST index on treenode_edge narrows each point to the edges whose bounds meet its box; the
# node's own location then decides. Two candidates are enough to call a position ambiguous.
NODES_NEAR = """
    SELECT p.idx, t.id, t.skeleton_id, t.parent_id
    FROM unnest(%(idx)s::int[], %(x)s::float8[], %(y)s::float8[], %(z)s::float8[]) AS p(idx, x, y, z)
    CROSS JOIN LATERAL (
        SELECT t.id, t.skeleton_id, t.parent_id
        FROM treenode_edge e
        JOIN treenode t ON t.id = e.id AND t.project_id = e.project_id
        WHERE e.project_id = %(project)s
          AND e.edge &&& ST_MakeLine(
              ST_MakePoint(p.x - %(margin)s::float8, p.y - %(margin)s::float8, p.z - %(margin)s::float8),
              ST_MakePoint(p.x + %(margin)s::float8, p.y + %(margin)s::float8, p.z + %(margin)s::float8))
          AND t.location_x BETWEEN p.x - %(margin)s::float8 AND p.x + %(margin)s::float8
          AND t.location_y BETWEEN p.y - %(margin)s::float8 AND p.y + %(margin)s::float8
          AND t.location_z BETWEEN p.z - %(margin)s::float8 AND p.z + %(margin)s::float8
        ORDER BY t.id
        LIMIT 2
    ) t
"""


def nodes_near(project_id: int, positions: list[tuple[float, float, float]], margin: float):
    """Up to two (treenode, skeleton, parent) of the project within `margin` of each position."""
    found = [[] for _ in positions]
    with connection.cursor() as cursor:
        for start in range(0, len(positions), BATCH):
            batch = positions[start:start + BATCH]
            cursor.execute(NODES_NEAR, {
                "project": project_id, "margin": margin, "idx": list(range(start, start + len(batch))),
                "x": [x for x, _, _ in batch], "y": [y for _, y, _ in batch], "z": [z for _, _, z in batch],
            })
            for index, treenode_id, skeleton_id, parent_id in cursor.fetchall():
                found[index].append((treenode_id, skeleton_id, parent_id))
    return found


def skip_reason(record: dict, nodes: list[list[tuple[int, int, int | None]]]) -> str | None:
    """Why a row cannot be placed, given the candidates of each of its points; None when it can."""
    for point, candidates in zip(record["points"], nodes):
        if not candidates:
            return "no_node_at_position"
        if len(candidates) > 1:
            return "ambiguous_position"
        if point.get("anchor") is not None and int(point["anchor"]) != candidates[0][0]:
            return "anchor_mismatch"
    if record["kind"] == MERGE_ERROR and nodes[0][0][2] is None:
        return "merge_node_is_root"
    if record["kind"] == SPLIT_ERROR and len({candidates[0][1] for candidates in nodes}) == 1:
        return "split_same_skeleton"
    return None


class Command(BaseCommand):
    help = ("Find the treenode at every review-item point (positions in nm, one JSON row per line, gzipped) "
            "and label the found nodes. Rows that cannot be placed are skipped and reported; when none can, "
            "nothing is labelled and the command fails.")

    def add_arguments(self, parser):
        parser.add_argument("--project-id", type=int, required=True)
        parser.add_argument("--label", required=True)
        parser.add_argument("--points", type=Path, required=True)
        parser.add_argument("--resolved", type=Path, required=True)
        parser.add_argument("--result", type=Path, required=True)
        parser.add_argument("--user-id", type=int, help="Owner of new label links; defaults to the first superuser")
        parser.add_argument("--margin-nm", type=float, default=5.0)

    def handle(self, *args, **options):
        project_id, result_path = options["project_id"], options["result"]
        user = (User.objects.get(pk=options["user_id"]) if options["user_id"]
                else User.objects.filter(is_superuser=True).order_by("pk").first())
        if user is None:
            raise CommandError("No user to own the labels; pass --user-id")
        with gzip.open(options["points"], "rt") as handle:
            records = [json.loads(line) for line in handle if line.strip()]

        with transaction.atomic():
            positions = [(point["x"], point["y"], point["z"]) for record in records for point in record["points"]]
            candidates = iter(nodes_near(project_id, positions, options["margin_nm"]))
            resolved, skipped = [], []
            for record in records:
                nodes = [next(candidates) for _ in record["points"]]
                reason = skip_reason(record, nodes)
                if reason:
                    skipped.append({"line": record["line"], "reason": reason})
                    continue
                resolved.append({**record, "points": [
                    {**point, "anchor_id": found[0][0]} for point, found in zip(record["points"], nodes)]})
            report = {"resolved": len(resolved), "skipped": skipped[:MAX_REPORTED], "skipped_count": len(skipped)}
            if not resolved:
                result_path.write_text(json.dumps({
                    "outcome": "failed", "category": "review_items_unresolved", **report,
                    "message": f"None of the {len(records)} rows could be placed on a node of project {project_id}",
                }))
                raise CommandError(f"No review item could be placed; {len(skipped)} rows skipped, nothing labelled")
            anchors = [point["anchor_id"] for record in resolved for point in record["points"]]
            labelled, already = label_treenodes(project_id, options["label"], anchors, user)

        with gzip.open(options["resolved"], "wt") as handle:
            handle.writelines(json.dumps(record) + "\n" for record in resolved)
        result = {"outcome": "succeeded", **report, "labelled": labelled, "already": already}
        result_path.write_text(json.dumps(result))
        self.stdout.write(json.dumps(result))
