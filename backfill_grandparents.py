"""One-off: fill comments.parent_parent_id for already-archived parents.

Uses reddit.info() (100 fullnames per request), so the whole DB is ~10 calls.
Safe to re-run; only rows still missing parent_parent_id are fetched.
"""

from __future__ import annotations

import db
from fetch_comments import _build_reddit, grandparent_comment_id


def main() -> None:
    conn = db.connect(must_exist=True)
    db.init_schema(conn)  # adds the column on an existing DB
    pids = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT parent_id FROM comments "
            "WHERE parent_id IS NOT NULL AND parent_parent_id IS NULL"
        )
    ]
    print(f"{len(pids)} parents to look up")
    reddit = _build_reddit()
    linked = 0
    for i in range(0, len(pids), 100):
        batch = pids[i : i + 100]
        for parent in reddit.info(fullnames=[f"t1_{p}" for p in batch]):
            gp = grandparent_comment_id(parent.parent_id)
            if gp:
                linked += conn.execute(
                    "UPDATE comments SET parent_parent_id = ? WHERE parent_id = ?",
                    (gp, parent.id),
                ).rowcount
        conn.commit()
        print(f"  {min(i + 100, len(pids))}/{len(pids)}")
    print(f"linked {linked} comment rows to a grandparent comment")


if __name__ == "__main__":
    main()
