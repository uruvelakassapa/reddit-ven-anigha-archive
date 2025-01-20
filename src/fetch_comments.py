import praw
import os
from dotenv import load_dotenv
from tqdm import tqdm
import sqlite3
import datetime
from typing import Any


def fetch_user_comments(
    reddit: praw.Reddit,
    username: str,
    limit: int,
    till_comment_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Fetches comments from a specified user, optionally stopping at a specific comment ID.

    Args:
        reddit: The PRAW Reddit instance.
        username: The Reddit username.
        limit: The max number of comments to fetch
        till_comment_id: The ID of the comment to stop fetching at (optional).

    Returns:
        A dictionary where keys are submission IDs and values are dictionaries
        containing submission details and a list of comments.
    """
    user = reddit.redditor(username)
    comments_by_submission = {}

    for comment in tqdm(
        user.comments.new(limit=limit),
        total=limit,
        desc=f"Fetching comments for {username}",
    ):
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

        comments_by_submission[submission_id]["comments"].append(comment)

    return comments_by_submission


def save_comments_to_db(conn: sqlite3.Connection, comments: dict[str, dict[str, Any]]):
    """
    Saves comments to the SQLite database.

    Args:
        conn: The SQLite database connection.
        comments: A dictionary of comments organized by submission.
    """
    c = conn.cursor()

    # Create tables if they don't exist
    c.execute(
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
    )
    """
    )

    c.execute(
        """
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        submission_id TEXT,
        author TEXT,
        created_utc INTEGER,
        parent_id TEXT,
        permalink TEXT,
        comment_body TEXT,
        updated_at REAL,
        FOREIGN KEY (submission_id) REFERENCES submissions (id),
        FOREIGN KEY (parent_id) REFERENCES comments (id)
    )
    """
    )

    for submission_id, submission_data in comments.items():
        c.execute(
            """
            INSERT INTO submissions (id, title, body, author, created_at, link, subreddit, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                body=excluded.body,
                author=excluded.author,
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
                datetime.datetime.now().timestamp(),
            ),
        )

        for comment in submission_data["comments"]:
            parent_id = comment.parent_id[3:] if not comment.is_root else None
            c.execute(
                """
            INSERT INTO comments (id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                submission_id=excluded.submission_id,
                author=excluded.author,
                created_utc=excluded.created_utc,
                parent_id=excluded.parent_id,
                permalink=excluded.permalink,
                comment_body=excluded.comment_body,
                updated_at=excluded.updated_at
            """,
                (
                    comment.id,
                    submission_id,
                    comment.author.name if comment.author else None,
                    comment.created_utc,
                    parent_id,
                    "https://www.reddit.com" + comment.permalink,
                    comment.body,
                    datetime.datetime.now().timestamp(),
                ),
            )

    conn.commit()


def get_latest_comment_id(conn: sqlite3.Connection) -> str | None:
    """
    Gets the ID of the latest comment stored in the database.

    Args:
        conn: The SQLite database connection.

    Returns:
        The ID of the latest comment, or None if the database is empty or the table doesn't exist.
    """
    c = conn.cursor()
    c.execute("SELECT id FROM comments ORDER BY created_utc DESC LIMIT 1")
    latest_comment = c.fetchone()
    return latest_comment[0] if latest_comment else None


def main():
    """
    Main function to fetch and store Reddit comments.
    """
    load_dotenv(".secrets")

    # Use environment variable or default to None for unlimited fetching.
    limit = int(os.getenv("LIMIT")) if os.getenv("LIMIT") else None

    # Set up your Reddit app credentials from environment variables
    reddit = praw.Reddit(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        user_agent=os.getenv("USER_AGENT"),
    )

    username = os.getenv("TARGET_USERNAME")
    till_last_comment = os.getenv("TILL_LAST_COMMENT", "true").lower() == "true"

    with sqlite3.connect("reddit_comments.db") as conn:
        if till_last_comment:
            last_comment_id = get_latest_comment_id(conn)
            comments_by_submission = fetch_user_comments(
                reddit, username, limit, till_comment_id=last_comment_id
            )
        else:
            comments_by_submission = fetch_user_comments(reddit, username, limit)

        if comments_by_submission:
            save_comments_to_db(conn, comments_by_submission)
            print(f"Database updated with comments organized by submission.")
        else:
            print(f"No new comments found for {username}.")


if __name__ == "__main__":
    main()
