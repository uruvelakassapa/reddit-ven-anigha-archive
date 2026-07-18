"""Fetch Reddit comments for target users and store them in SQLite."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import praw
from dotenv import load_dotenv
from tqdm import tqdm

import db

load_dotenv()

TARGET_USERNAMES = ["Bhikkhu_Anigha", "Sister_Medhini"]
LIMIT = int(os.getenv("LIMIT")) if os.getenv("LIMIT") else None


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _build_reddit() -> praw.Reddit:
    return praw.Reddit(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        user_agent=os.getenv("USER_AGENT"),
    )


def fetch_user_comments(
    reddit: praw.Reddit,
    username: str,
    till_comment_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Fetch comments for a single user, grouped by submission id."""
    print(f"Fetching comments for user: {username}")
    user = reddit.redditor(username)
    comments_by_submission: Dict[str, Dict[str, Any]] = {}

    for comment in tqdm(user.comments.new(limit=LIMIT), desc=f"User {username}"):
        if comment.id == till_comment_id:
            break

        submission_id = comment.submission.id
        if submission_id not in comments_by_submission:
            submission = comment.submission
            comments_by_submission[submission_id] = {
                "title": submission.title,
                "body": submission.selftext,
                "author": submission.author.name if submission.author else None,
                "created_at": submission.created_utc,
                "link": submission.url,
                "subreddit": submission.subreddit_name_prefixed,
                "comments": [],
            }

        comment_data: Dict[str, Any] = {
            "id": comment.id,
            "author": comment.author.name if comment.author else "[deleted]",
            "created_utc": comment.created_utc,
            "permalink": f"https://www.reddit.com{comment.permalink}",
            "body": comment.body,
            "parent": None,
        }

        if not comment.is_root:
            try:
                parent = comment.parent()
                if isinstance(parent, praw.models.Comment):
                    parent_author_obj = getattr(parent, "author", None)
                    comment_data["parent"] = {
                        "id": parent.id,
                        "author": parent_author_obj.name if parent_author_obj else "[deleted]",
                        "body": getattr(parent, "body", "[unavailable]"),
                        "permalink": f"https://www.reddit.com{getattr(parent, 'permalink', '')}",
                    }
            except Exception as e:
                print(f"Warning: Could not fetch parent for comment {comment.id}. Error: {e}")

        comments_by_submission[submission_id]["comments"].append(comment_data)

    return comments_by_submission


def merge_by_submission(
    target: Dict[str, Dict[str, Any]],
    source: Dict[str, Dict[str, Any]],
) -> None:
    """
    Merge source into target by submission id.

    Comments are merged by id so two target users on the same thread
    both keep their comments (dict.update would drop one user's list).
    """
    for submission_id, data in source.items():
        if submission_id not in target:
            target[submission_id] = {
                **data,
                "comments": list(data["comments"]),
            }
            continue

        existing: List[Dict[str, Any]] = target[submission_id]["comments"]
        seen_ids = {c["id"] for c in existing}
        for comment in data["comments"]:
            if comment["id"] not in seen_ids:
                existing.append(comment)
                seen_ids.add(comment["id"])


def main() -> None:
    till_last_comment = _env_bool("TILL_LAST_COMMENT", default=True)
    reddit = _build_reddit()
    conn = db.connect()
    if conn is None:
        raise SystemExit(1)

    db.init_schema(conn)

    all_by_submission: Dict[str, Dict[str, Any]] = {}
    print(f"Processing users: {', '.join(TARGET_USERNAMES)}")

    try:
        for username in TARGET_USERNAMES:
            last_comment_id = None
            if till_last_comment:
                last_comment_id = db.get_latest_comment_id_for_user(conn, username)
                if last_comment_id:
                    print(
                        f"Fetching comments for {username} newer than comment ID: {last_comment_id}"
                    )
                else:
                    print(
                        f"No previous comments found for {username} in DB. "
                        "Fetching all available comments."
                    )
            else:
                print(
                    f"Fetching all available comments for {username} "
                    "(TILL_LAST_COMMENT is false)."
                )

            try:
                user_comments = fetch_user_comments(
                    reddit, username, till_comment_id=last_comment_id
                )
                merge_by_submission(all_by_submission, user_comments)
            except Exception as e:
                print(f"Error fetching comments for user {username}: {e}")

        if all_by_submission:
            db.upsert_fetched_data(conn, all_by_submission)
            print(f"Database updated with comments for users: {', '.join(TARGET_USERNAMES)}.")
        else:
            print(f"No new comments found for users: {', '.join(TARGET_USERNAMES)}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
