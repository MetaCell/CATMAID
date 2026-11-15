"""NeuroGlass review items: treenodes they point at carry a reserved label (NGLASS-2386).

Only NeuroGlass imports add or remove the label. Deleting a labelled treenode needs the request
to confirm it, so NeuroGlass can tell the user how many review items would lose their node.
"""
from typing import Iterable

from django.db import connection

from catmaid.control.common import get_request_bool
from catmaid.error import ClientError

REVIEW_ITEM_LABEL = "neuroglass:review-item"
MAX_REPORTED_IDS = 1000
CONFIRM_PARAMETER = "confirm_review_items"


class ReviewItemNodesProtected(ClientError):
    status_code = 409

    def __init__(self, treenode_ids: list[int]):
        ids = sorted(treenode_ids)
        super().__init__(f"{len(ids)} treenodes carry review items; "
                         f"resend with {CONFIRM_PARAMETER}=true to delete them")
        self.meta = {"label": REVIEW_ITEM_LABEL, "count": len(ids), "treenode_ids": ids[:MAX_REPORTED_IDS]}


def labelled_treenode_ids(project_id: int, *, treenode_ids: Iterable[int] = (),
                          skeleton_ids: Iterable[int] = ()) -> list[int]:
    treenode_ids, skeleton_ids = [int(i) for i in treenode_ids], [int(i) for i in skeleton_ids]
    if not treenode_ids and not skeleton_ids:
        return []
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT t.id
            FROM treenode t
            JOIN treenode_class_instance tci ON tci.treenode_id = t.id
            JOIN relation r ON r.id = tci.relation_id AND r.relation_name = 'labeled_as'
            JOIN class_instance label ON label.id = tci.class_instance_id
            WHERE t.project_id = %(project)s AND tci.project_id = %(project)s AND label.name = %(label)s
              AND (t.id = ANY(%(treenodes)s::bigint[]) OR t.skeleton_id = ANY(%(skeletons)s::bigint[]))
            ORDER BY t.id
        """, {"project": project_id, "label": REVIEW_ITEM_LABEL, "treenodes": treenode_ids, "skeletons": skeleton_ids})
        return [row[0] for row in cursor.fetchall()]


def guard_review_item_nodes(project_id: int, *, confirmed: bool, treenode_ids: Iterable[int] = (),
                            skeleton_ids: Iterable[int] = ()) -> None:
    if confirmed:
        return
    labelled = labelled_treenode_ids(project_id, treenode_ids=treenode_ids, skeleton_ids=skeleton_ids)
    if labelled:
        raise ReviewItemNodesProtected(labelled)


def review_items_confirmed(request) -> bool:
    return get_request_bool(request.POST, CONFIRM_PARAMETER, False)


def reject_reserved_label_change(added: Iterable[str], removed: Iterable[str]) -> None:
    if REVIEW_ITEM_LABEL in set(added) | set(removed):
        from catmaid.control.authentication import PermissionError
        raise PermissionError(f"The {REVIEW_ITEM_LABEL} label is managed by NeuroGlass imports")
