"""SQLite helpers for the Reddit archive."""

from __future__ import annotations

import datetime
import sqlite3
import sys
from typing import Any, Dict, List, Optional

DB_DEFAULT = "reddit_comments.db"

# Reddit (and the fetch script) use these for deleted / missing content.
# On upsert conflict, keep the existing DB value when the incoming value matches.
_TOMBSTONE_MARKERS = ("[deleted]", "[removed]", "[unavailable]")


def _sql_preserve_if_tombstone(column: str) -> str:
    """SQL CASE: keep existing column when excluded value is null/empty/deleted."""
    markers = ", ".join(f"'{m}'" for m in _TOMBSTONE_MARKERS)
    return (
        f"CASE WHEN excluded.{column} IS NULL "
        f"OR TRIM(excluded.{column}) = '' "
        f"OR LOWER(TRIM(excluded.{column})) IN ({markers}) "
        f"THEN {column} ELSE excluded.{column} END"
    )


def connect(
    db_path: str = DB_DEFAULT,
    *,
    must_exist: bool = False,
) -> Optional[sqlite3.Connection]:
    """Open a connection with Row factory. Returns None if must_exist and missing."""
    import os

    if must_exist and not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}", file=sys.stderr)
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}", file=sys.stderr)
        return None


def init_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they do not exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS submissions (
            id TEXT PRIMARY KEY,
            title TEXT,
            body TEXT,
            author TEXT,
            created_at REAL,
            link TEXT,
            subreddit TEXT,
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            submission_id TEXT,
            author TEXT,
            created_utc REAL,
            parent_id TEXT,
            permalink TEXT,
            comment_body TEXT,
            parent_author TEXT,
            parent_body TEXT,
            parent_permalink TEXT,
            updated_at REAL,
            FOREIGN KEY (submission_id) REFERENCES submissions (id)
        );
        """
    )
    conn.commit()


def upsert_fetched_data(
    conn: sqlite3.Connection,
    comments_by_submission: Dict[str, Dict[str, Any]],
) -> None:
    """
    Insert or update submissions and comments.

    Content fields are not overwritten with deletion markers if real text
    was already archived (e.g. OP deletes their account later).
    """
    now = datetime.datetime.now(datetime.UTC).timestamp()
    c = conn.cursor()

    for submission_id, submission_data in comments_by_submission.items():
        c.execute(
            f"""
            INSERT INTO submissions (id, title, body, author, created_at, link, subreddit, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title={_sql_preserve_if_tombstone("title")},
                body={_sql_preserve_if_tombstone("body")},
                author={_sql_preserve_if_tombstone("author")},
                created_at=excluded.created_at,
                link=excluded.link,
                subreddit=excluded.subreddit,
                updated_at=excluded.updated_at
            """,
            (
                submission_id,
                submission_data["title"],
                submission_data["body"],
                submission_data["author"],
                submission_data["created_at"],
                submission_data["link"],
                submission_data["subreddit"],
                now,
            ),
        )

        for comment_data in submission_data["comments"]:
            parent_info = comment_data.get("parent")
            parent_id = parent_info["id"] if parent_info else None
            parent_author = parent_info["author"] if parent_info else None
            parent_body = parent_info["body"] if parent_info else None
            parent_permalink = parent_info["permalink"] if parent_info else None

            c.execute(
                f"""
                INSERT INTO comments (
                    id, submission_id, author, created_utc, parent_id, permalink,
                    comment_body, parent_author, parent_body, parent_permalink, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    submission_id=excluded.submission_id,
                    author={_sql_preserve_if_tombstone("author")},
                    created_utc=excluded.created_utc,
                    parent_id={_sql_preserve_if_tombstone("parent_id")},
                    permalink=excluded.permalink,
                    comment_body={_sql_preserve_if_tombstone("comment_body")},
                    parent_author={_sql_preserve_if_tombstone("parent_author")},
                    parent_body={_sql_preserve_if_tombstone("parent_body")},
                    parent_permalink={_sql_preserve_if_tombstone("parent_permalink")},
                    updated_at=excluded.updated_at
                """,
                (
                    comment_data["id"],
                    submission_id,
                    comment_data["author"],
                    comment_data["created_utc"],
                    parent_id,
                    comment_data["permalink"],
                    comment_data["body"],
                    parent_author,
                    parent_body,
                    parent_permalink,
                    now,
                ),
            )

    conn.commit()


def get_latest_comment_id_for_user(conn: sqlite3.Connection, username: str) -> Optional[str]:
    """Return the newest stored comment id for username, or None."""
    try:
        row = conn.execute(
            "SELECT id FROM comments WHERE author = ? ORDER BY created_utc DESC LIMIT 1",
            (username,),
        ).fetchone()
        return row["id"] if row else None
    except sqlite3.OperationalError:
        print("Warning: Comments table not found or query failed. Fetching all comments for user.")
        return None


def fetch_submissions(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """All submissions, newest first."""
    return list(
        conn.execute("SELECT * FROM submissions ORDER BY created_at DESC")
    )


def fetch_comments_for_submission(
    conn: sqlite3.Connection,
    submission_id: str,
) -> List[sqlite3.Row]:
    """All comments for a submission, oldest first (includes parent context columns)."""
    return list(
        conn.execute(
            """
            SELECT id, author, created_utc, parent_id, permalink, comment_body,
                   parent_author, parent_body, parent_permalink
            FROM comments
            WHERE submission_id = ?
            ORDER BY created_utc
            """,
            (submission_id,),
        )
    )
