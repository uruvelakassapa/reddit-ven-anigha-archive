"""Nested comment threads, including parents that are not in the archive."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ThreadNode:
    id: str
    parent_id: Optional[str]
    user: str
    content: str
    url: str
    created_at: float
    parent_user: Optional[str] = None
    parent_content: Optional[str] = None
    synthetic: bool = False
    children: List["ThreadNode"] = field(default_factory=list)


def _node_from_row(row) -> ThreadNode:
    return ThreadNode(
        id=row["id"],
        parent_id=row["parent_id"],
        user=row["author"] or "[deleted]",
        content=row["comment_body"] or "",
        url=row["permalink"] or "#",
        created_at=row["created_utc"] or 0,
        parent_user=row["parent_author"],
        parent_content=row["parent_body"],
    )


def _synthetic_parent(row) -> ThreadNode:
    body = row["parent_body"]
    return ThreadNode(
        id=row["parent_id"],
        parent_id=None,
        user=row["parent_author"] or "[deleted]",
        content="" if body is None else body,
        url=row["parent_permalink"] or "#",
        created_at=row["created_utc"] or 0,
        synthetic=True,
    )


def build_comment_threads(
    comments,
    *,
    include_missing_parents: bool = False,
) -> List[ThreadNode]:
    """Build nested threads from a flat comment list; return ordered roots.

    When include_missing_parents is True, a reply whose parent is not in
    `comments` gets a synthetic parent node (from parent_* columns) so the
    parent is a sibling card/list item rather than a quote prefix.
    """
    nodes: Dict[str, ThreadNode] = {}
    for row in comments:
        nodes[row["id"]] = _node_from_row(row)

    if include_missing_parents:
        for row in comments:
            pid = row["parent_id"]
            if not pid or pid in nodes:
                continue
            nodes[pid] = _synthetic_parent(row)

    top_level: List[ThreadNode] = []
    orphans: List[ThreadNode] = []
    for node in nodes.values():
        if node.parent_id and node.parent_id in nodes:
            nodes[node.parent_id].children.append(node)
        elif not node.parent_id:
            top_level.append(node)
        else:
            orphans.append(node)

    for node in nodes.values():
        node.children.sort(key=lambda n: n.created_at)
    top_level.sort(key=lambda n: n.created_at)
    orphans.sort(key=lambda n: n.created_at)
    return top_level + orphans
