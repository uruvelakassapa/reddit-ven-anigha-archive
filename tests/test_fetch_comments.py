from praw.models import Comment, Submission, Redditor
from typing import Any
from unittest.mock import patch, MagicMock
import datetime
import pytest
import re
import sqlite3

from src.fetch_comments import (
    fetch_user_comments,
    save_comments_to_db,
    get_latest_comment_id,
)

# --- Mock Data and Objects ---

MOCK_DATETIME = datetime.datetime(2023, 10, 27, 10, 0, 0, tzinfo=datetime.UTC)
MOCK_USERNAME = "testuser"
MOCK_COMMENT_ID = "comment1"


def get_ints_at_end(string):
    """Gets all the integers at the end of a string."""

    if match := re.search(r"\d+$", string):
        return int(match.group())
    else:
        return None


def create_mock_submission(submission_id: str, **kwargs: Any) -> MagicMock:
    """Helper function to create mock PRAW Submission objects with sensible defaults."""
    mock_submission = MagicMock(spec=Submission)
    mock_submission.id = submission_id
    mock_submission.title = "title_" + submission_id
    mock_submission.selftext = "body_" + submission_id
    mock_submission.author = MagicMock(spec=Redditor)
    mock_submission.author.name = "author_" + submission_id
    mock_submission.created_utc = (
        kwargs.get("created_utc")
        if kwargs.get("created_utc") is not None
        else (MOCK_DATETIME - datetime.timedelta(days=get_ints_at_end(submission_id)))
    ).timestamp()  # Default time is based on mock id
    mock_submission.url = f"https://reddit.com/r/subreddit_{submission_id}/comments/{submission_id}/"
    mock_submission.subreddit_name_prefixed = "r/subreddit_" + submission_id
    return mock_submission


def create_mock_comment(
    submission: MagicMock,
    comment_id: str,
    parent_id: str | None = None,
    is_root: bool = True,
    **kwargs: Any,
) -> MagicMock:
    """Helper function to create mock PRAW Comment objects with sensible defaults."""
    mock_comment = MagicMock(spec=Comment)
    mock_comment.id = comment_id
    mock_comment.parent_id = parent_id if parent_id else f"t3_{submission.id}"  # Simulate PRAW's structure
    mock_comment.is_root = is_root
    mock_comment.author = MagicMock(spec=Redditor)
    mock_comment.author.name = "author_" + comment_id
    mock_comment.submission = submission  # Use the provided submission object
    mock_comment.permalink = f"/r/{submission.subreddit_name_prefixed}/comments/{submission.id}/{comment_id}/"
    mock_comment.created_utc = (
        kwargs.get("created_utc")
        if kwargs.get("created_utc") is not None
        else (MOCK_DATETIME - datetime.timedelta(days=get_ints_at_end(comment_id))).timestamp()
    )  # Default time is based on mock id
    mock_comment.body = "body_" + comment_id
    return mock_comment


# --- Fixtures ---


@pytest.fixture
def mock_reddit():
    """Fixture to provide a mocked PRAW Reddit instance."""
    mock_reddit = MagicMock()
    mock_submission1 = create_mock_submission("submission1")
    mock_comments = [
        create_mock_comment(mock_submission1, "comment1"),
        create_mock_comment(mock_submission1, "comment2", parent_id="t3_submission1", is_root=False),
    ]
    mock_reddit.redditor.return_value.comments.new.return_value = mock_comments
    return mock_reddit


@pytest.fixture
def in_memory_db():
    """Fixture to provide an in-memory SQLite database connection."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


# --- Test Cases ---


def test_fetch_user_comments_no_existing(mock_reddit: MagicMock):
    comments = fetch_user_comments(mock_reddit, MOCK_USERNAME, 100)

    assert len(comments) == 1
    assert "submission1" in comments
    assert len(comments["submission1"]["comments"]) == 2


def test_fetch_user_comments_till_existing(mock_reddit: MagicMock):
    comments = fetch_user_comments(mock_reddit, MOCK_USERNAME, 100, till_comment_id="comment2")

    assert len(comments) == 1
    assert "submission1" in comments
    assert len(comments["submission1"]["comments"]) == 1


def test_fetch_user_comments_empty(mock_reddit: MagicMock):
    mock_reddit.redditor.return_value.comments.new.return_value = []
    comments = fetch_user_comments(mock_reddit, MOCK_USERNAME, 100)
    assert len(comments) == 0


def test_save_comments_to_db(in_memory_db: sqlite3.Connection):
    mock_submission1 = create_mock_submission("submission1")
    mock_submission2 = create_mock_submission("submission2")
    mock_comments_data = {
        "submission1": {
            "title": "Submission Title 1",
            "body": "Submission Body 1",
            "author": "Author1",
            "created_at": MOCK_DATETIME.timestamp(),
            "link": "https://reddit.com/r/subreddit1/comments/submission1/",
            "subreddit": "r/subreddit1",
            "comments": [
                create_mock_comment(mock_submission1, "comment1", created_utc=MOCK_DATETIME.timestamp()),
            ],
        },
        "submission2": {
            "title": "Submission Title 2",
            "body": "Submission Body 2",
            "author": "Author2",
            "created_at": MOCK_DATETIME.timestamp() - 86400,
            "link": "https://reddit.com/r/subreddit2/comments/submission2/",
            "subreddit": "r/subreddit2",
            "comments": [
                create_mock_comment(
                    mock_submission2,
                    "comment2",
                    parent_id="t3_submission1",
                    is_root=False,
                    created_utc=MOCK_DATETIME.timestamp() - 86400,
                ),
            ],
        },
    }

    with patch("src.fetch_comments.datetime.datetime") as mock_dt:
        mock_dt.now.return_value = MOCK_DATETIME
        save_comments_to_db(in_memory_db, mock_comments_data)

    cursor = in_memory_db.cursor()
    cursor.execute("SELECT * FROM submissions")
    submissions = cursor.fetchall()
    assert len(submissions) == 2

    cursor.execute("SELECT * FROM comments")
    comments = cursor.fetchall()
    assert len(comments) == 2

    # Test upsert (update existing comment)
    mock_comments_data["submission1"]["comments"][0].body = "Updated Comment Body"
    with patch("src.fetch_comments.datetime.datetime") as mock_dt:
        mock_dt.now.return_value = MOCK_DATETIME
        save_comments_to_db(in_memory_db, mock_comments_data)

    cursor.execute("SELECT comment_body FROM comments WHERE id = 'comment1'")
    updated_comment_body = cursor.fetchone()[0]
    assert updated_comment_body == "Updated Comment Body"


def test_get_latest_comment_id(in_memory_db: sqlite3.Connection):
    # Create tables
    c = in_memory_db.cursor()
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

    # Test with empty database
    latest_comment_id = get_latest_comment_id(in_memory_db)
    assert latest_comment_id is None

    # Test with populated database
    mock_submission1 = create_mock_submission("submission1")
    mock_comments_data = {
        "submission1": {
            "title": "Submission Title 1",
            "body": "Submission Body 1",
            "author": "Author1",
            "created_at": MOCK_DATETIME.timestamp(),
            "link": "https://reddit.com/r/subreddit1/comments/submission1/",
            "subreddit": "r/subreddit1",
            "comments": [
                create_mock_comment(mock_submission1, "comment1", created_utc=MOCK_DATETIME.timestamp()),
                create_mock_comment(
                    mock_submission1,
                    "comment2",
                    created_utc=MOCK_DATETIME.timestamp() - 86400,
                ),
            ],
        }
    }
    with patch("src.fetch_comments.datetime.datetime") as mock_dt:
        mock_dt.now.return_value = MOCK_DATETIME
        save_comments_to_db(in_memory_db, mock_comments_data)

    latest_comment_id = get_latest_comment_id(in_memory_db)
    assert latest_comment_id == "comment1"


@patch("src.fetch_comments.praw")
@patch("src.fetch_comments.os")
@patch("src.fetch_comments.fetch_user_comments")
@patch("src.fetch_comments.get_latest_comment_id")
@patch("src.fetch_comments.save_comments_to_db")
@patch("src.fetch_comments.load_dotenv")
def test_main(
    mock_load_dotenv: MagicMock,
    mock_save_comments_to_db: MagicMock,
    mock_get_latest_comment_id: MagicMock,
    mock_fetch_user_comments: MagicMock,
    mock_os: MagicMock,
    mock_praw: MagicMock,
):
    # Import main to load in the patched modules.
    from src.fetch_comments import main

    mock_os.getenv.side_effect = lambda key, default=None: {
        "CLIENT_ID": "test_client_id",
        "CLIENT_SECRET": "test_client_secret",
        "USER_AGENT": "test_user_agent",
        "TARGET_USERNAME": "testuser",
        "TILL_LAST_COMMENT": "true",
        "LIMIT": "10",
    }.get(key, default)

    mock_get_latest_comment_id.return_value = MOCK_COMMENT_ID

    mock_submission = create_mock_submission("submission1")
    mock_comments = {
        "submission1": {
            "title": "Test Submission",
            "body": "Test Body",
            "author": "testuser",
            "created_at": MOCK_DATETIME.timestamp(),
            "link": "https://reddit.com/r/testsubreddit/comments/submission1/",
            "subreddit": "r/testsubreddit",
            "comments": [create_mock_comment(mock_submission, MOCK_COMMENT_ID)],
        }
    }
    mock_fetch_user_comments.return_value = mock_comments

    main()

    mock_load_dotenv.assert_called_once()
    mock_praw.Reddit.assert_called_once_with(
        client_id="test_client_id",
        client_secret="test_client_secret",
        user_agent="test_user_agent",
    )
    mock_fetch_user_comments.assert_called_once_with(
        mock_praw.Reddit.return_value, "testuser", 10, till_comment_id=MOCK_COMMENT_ID
    )
    mock_save_comments_to_db.assert_called_once()
    args, _ = mock_save_comments_to_db.call_args
    assert len(args) == 2
    assert isinstance(args[0], sqlite3.Connection)
    assert args[1] == mock_comments
    mock_get_latest_comment_id.assert_called_once()

    # Test the case where TILL_LAST_COMMENT is false
    mock_os.getenv.side_effect = lambda key, default=None: {
        "CLIENT_ID": "test_client_id",
        "CLIENT_SECRET": "test_client_secret",
        "USER_AGENT": "test_user_agent",
        "TARGET_USERNAME": "testuser",
        "TILL_LAST_COMMENT": "false",
        "LIMIT": "10",
    }.get(key, default)
    mock_get_latest_comment_id.reset_mock()
    mock_fetch_user_comments.reset_mock()

    main()

    mock_fetch_user_comments.assert_called_once_with(mock_praw.Reddit.return_value, "testuser", 10)
    mock_get_latest_comment_id.assert_not_called()
