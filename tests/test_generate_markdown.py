from typing import Any
import datetime
import sqlite3
from pathlib import Path
import pytest

from src.generate_markdown import (
    create_intended_md_from_submission,
    create_intended_md_from_thread,
    create_non_indented_md_from_submission,
    create_non_indented_md_from_thread,
    create_thread_dicts,
    metablock_template,
    save_comments_to_markdown,
    sanitize_markdown_content,
    CommentType,
)


# Helper function to set up an in-memory database
def setup_in_memory_db(
    submissions_data: list[dict[str, Any]], comments_data: list[dict[str, Any]]
) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create submissions table
    cursor.execute(
        """
        CREATE TABLE submissions (
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

    # Create comments table
    cursor.execute(
        """
        CREATE TABLE comments (
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

    # Insert data into submissions table
    for sub in submissions_data:
        cursor.execute(
            """
            INSERT INTO submissions (id, title, body, author, created_at, link, subreddit, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sub["id"],
                sub["title"],
                sub["body"],
                sub["author"],
                sub["created_at"],
                sub["link"],
                sub["subreddit"],
                sub["updated_at"],
            ),
        )

    # Insert data into comments table
    for comm in comments_data:
        cursor.execute(
            """
            INSERT INTO comments (id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comm["id"],
                comm["submission_id"],
                comm["author"],
                comm["created_utc"],
                comm["parent_id"],
                comm["permalink"],
                comm["comment_body"],
                comm["updated_at"],
            ),
        )

    conn.commit()
    return conn


# --- Test Fixtures ---
@pytest.fixture
def sample_db_data() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # Sample submissions data (mimicking structure from fetch_comments.py)
    submissions_data = [
        {
            "id": "sub1",
            "title": "Submission Title 1",
            "body": "Submission Body 1",
            "author": "Author1",
            "created_at": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC).timestamp(),
            "link": "https://reddit.com/r/subreddit1/comments/sub1",
            "subreddit": "r/subreddit1",
            "updated_at": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "sub2",
            "title": "Submission Title 2 # Header",
            "body": "Submission Body 2\n\n# Another Header",
            "author": "Author2",
            "created_at": datetime.datetime(2023, 2, 1, tzinfo=datetime.UTC).timestamp(),
            "link": "https://reddit.com/r/subreddit2/comments/sub2",
            "subreddit": "r/subreddit2",
            "updated_at": datetime.datetime(2023, 2, 1, tzinfo=datetime.UTC).timestamp(),
        },
    ]

    # Sample comments data
    comments_data = [
        {
            "id": "com1",
            "submission_id": "sub1",
            "author": "Commenter1",
            "created_utc": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC).timestamp(),
            "parent_id": None,
            "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/",
            "comment_body": "Comment 1 on Submission 1",
            "updated_at": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "com2",
            "submission_id": "sub1",
            "author": "Commenter2",
            "created_utc": datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC).timestamp(),
            "parent_id": "com1",
            "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/",
            "comment_body": "Comment 2 (reply to Comment 1)",
            "updated_at": datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "com3",
            "submission_id": "sub2",
            "author": "Commenter3",
            "created_utc": datetime.datetime(2023, 2, 2, tzinfo=datetime.UTC).timestamp(),
            "parent_id": None,
            "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/",
            "comment_body": "Comment 3 on Submission 2",
            "updated_at": datetime.datetime(2023, 2, 2, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "com4",
            "submission_id": "sub2",
            "author": "Commenter4",
            "created_utc": datetime.datetime(2023, 2, 3, tzinfo=datetime.UTC).timestamp(),
            "parent_id": "com3",
            "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/",
            "comment_body": "Comment 4 (reply to Comment 3)\n\n# Header in comment",
            "updated_at": datetime.datetime(2023, 2, 3, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "com5",
            "submission_id": "sub2",
            "author": "Commenter5",
            "created_utc": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
            "parent_id": "com4",
            "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/",
            "comment_body": "Comment 5 (reply to Comment 4)",
            "updated_at": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
        },
        {
            "id": "com7",
            "submission_id": "sub2",
            "author": "Commenter7",
            "created_utc": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
            "parent_id": "com6",
            "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_7/",
            "comment_body": "Comment 7 (orphaned)",
            "updated_at": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
        },
    ]

    return submissions_data, comments_data


@pytest.fixture
def in_memory_db(
    sample_db_data: tuple[list[dict[str, Any]], list[dict[str, Any]]],
) -> sqlite3.Connection:
    submissions_data, comments_data = sample_db_data
    return setup_in_memory_db(submissions_data, comments_data)


# --- Test Cases ---


def test_create_thread_dicts(
    in_memory_db: sqlite3.Connection,
):
    cursor = in_memory_db.cursor()
    cursor.execute(
        """
        SELECT
            id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at
        FROM comments
        ORDER BY created_utc
        """
    )
    comments_data = cursor.fetchall()
    comments_dicts = create_thread_dicts(comments_data)

    # Assertions to check if the structure is built correctly
    assert len(comments_dicts) == 3  # Two top-level threads, one orphan

    # Find top level threads (comments that have no parent)
    top_level_comments = [t for t in comments_dicts if t["parent_id"] is None]

    # Test top level threads
    assert top_level_comments[0]["id"] == "com3"
    assert len(top_level_comments[0]["children"]) == 1
    assert top_level_comments[0]["children"][0]["id"] == "com4"

    assert top_level_comments[1]["id"] == "com1"
    assert len(top_level_comments[1]["children"]) == 1
    assert top_level_comments[1]["children"][0]["id"] == "com2"

    # Find orphan threads
    orphan_comments = comments_dicts[len(top_level_comments) :]
    assert len(orphan_comments) == 1
    assert orphan_comments[0]["id"] == "com7"


def test_create_intended_md_from_submission(
    in_memory_db: sqlite3.Connection,
    sample_db_data: tuple[list[dict[str, Any]], list[dict[str, Any]]],
):
    submissions_data, _ = sample_db_data
    cursor = in_memory_db.cursor()
    for submission_dict in submissions_data:
        result = create_intended_md_from_submission(cursor, submission_dict)
        if submission_dict["id"] == "sub1":
            expected = (
                "**r/subreddit1** | Posted by Author1 _2024-01-01 00:00:00_\n"
                "### [Submission Title 1](https://reddit.com/r/subreddit1/comments/sub1)\n\n"
                "Submission Body 1\n\n"
                "- **[Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/)** _2024-01-02 00:00:00_:\n\n"
                "    Comment 1 on Submission 1\n"
                "    - **[Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/)** _2024-01-03 00:00:00_:\n\n"
                "        Comment 2 (reply to Comment 1)\n"
                "\n---\n\n"
            )
        elif submission_dict["id"] == "sub2":
            expected = (
                "**r/subreddit2** | Posted by Author2 _2023-02-01 00:00:00_\n"
                "### [Submission Title 2 # Header](https://reddit.com/r/subreddit2/comments/sub2)\n\n"
                "Submission Body 2\n\n"
                "# Another Header\n\n"
                "- **[Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/)** _2023-02-02 00:00:00_:\n\n"
                "    Comment 3 on Submission 2\n"
                "    - **[Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/)** _2023-02-03 00:00:00_:\n\n"
                "        Comment 4 (reply to Comment 3)\n\n"
                "        # Header in comment\n"
                "        - **[Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/)** _2023-02-04 00:00:00_:\n\n"
                "            Comment 5 (reply to Comment 4)\n"
                "- **[Commenter7](https://www.reddit.com/r/subreddit2/comments/sub2/comment_7/)** _2023-02-04 00:00:00_ *(in reply to a comment not included)*:\n\n"
                "    Comment 7 (orphaned)\n"
                "\n---\n\n"
            )

        assert result == expected


def test_create_intended_md_from_thread():
    comment1 = {
        "id": "com1",
        "parent_id": None,
        "author": "Commenter1",
        "comment_body": "Comment 1 on Submission 1",
        "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/",
        "created_utc": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.PARENT,
    }
    comment2 = {
        "id": "com2",
        "parent_id": "com1",
        "author": "Commenter2",
        "comment_body": "Comment 2 (reply to Comment 1)",
        "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/",
        "created_utc": datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment1["children"].append(comment2)

    result = create_intended_md_from_thread(comment1)
    expected = (
        "- **[Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/)** _2024-01-02 00:00:00_:\n\n"
        "    Comment 1 on Submission 1\n"
        "    - **[Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/)** _2024-01-03 00:00:00_:\n\n"
        "        Comment 2 (reply to Comment 1)\n"
    )
    assert result == expected

    comment3 = {
        "id": "com3",
        "parent_id": None,
        "author": "Commenter3",
        "comment_body": "Comment 3 on Submission 2",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/",
        "created_utc": datetime.datetime(2023, 2, 2, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.PARENT,
    }
    comment4 = {
        "id": "com4",
        "parent_id": "com3",
        "author": "Commenter4",
        "comment_body": "Comment 4 (reply to Comment 3)\n\n# Header in comment",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/",
        "created_utc": datetime.datetime(2023, 2, 3, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment5 = {
        "id": "com5",
        "parent_id": "com4",
        "author": "Commenter5",
        "comment_body": "Comment 5 (reply to Comment 4)",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/",
        "created_utc": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment3["children"].append(comment4)
    comment4["children"].append(comment5)

    result = create_intended_md_from_thread(comment3)
    expected = (
        "- **[Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/)** _2023-02-02 00:00:00_:\n\n"
        "    Comment 3 on Submission 2\n"
        "    - **[Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/)** _2023-02-03 00:00:00_:\n\n"
        "        Comment 4 (reply to Comment 3)\n\n"
        "        # Header in comment\n"
        "        - **[Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/)** _2023-02-04 00:00:00_:\n\n"
        "            Comment 5 (reply to Comment 4)\n"
    )
    assert result == expected


def test_sanitize_markdown_content():
    content = "This is some text.\n# This is a header.\n## This is a subheader."
    expected = "This is some text.\n\\# This is a header.\n\\## This is a subheader."
    assert sanitize_markdown_content(content) == expected


def test_create_non_indented_md_from_submission(
    in_memory_db: sqlite3.Connection,
    sample_db_data: tuple[list[dict[str, Any]], list[dict[str, Any]]],
):
    submissions_data, _ = sample_db_data
    cursor = in_memory_db.cursor()
    for submission_dict in submissions_data:
        result = create_non_indented_md_from_submission(cursor, submission_dict)
        if submission_dict["id"] == "sub1":
            expected = (
                "## [Submission Title 1](https://reddit.com/r/subreddit1/comments/sub1)\n"
                "**Subreddit**: r/subreddit1 | **Posted by**: Author1 _2024-01-01 00:00:00_\n\n"
                "Submission Body 1\n\n"
                "### Comment by [Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/) on 2024-01-02 00:00:00\n\n"
                "Comment 1 on Submission 1\n\n"
                "#### Comment by [Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/) on 2024-01-03 00:00:00\n\n"
                "Comment 2 (reply to Comment 1)\n\n"
            )
        elif submission_dict["id"] == "sub2":
            expected = (
                "## [Submission Title 2 # Header](https://reddit.com/r/subreddit2/comments/sub2)\n"
                "**Subreddit**: r/subreddit2 | **Posted by**: Author2 _2023-02-01 00:00:00_\n\n"
                "Submission Body 2\n\n\\# Another Header\n\n"
                "### Comment by [Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/) on 2023-02-02 00:00:00\n\n"
                "Comment 3 on Submission 2\n\n"
                "#### Comment by [Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/) on 2023-02-03 00:00:00\n\n"
                "Comment 4 (reply to Comment 3)\n\n\\# Header in comment\n\n"
                "##### Comment by [Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/) on 2023-02-04 00:00:00\n\n"
                "Comment 5 (reply to Comment 4)\n\n"
                "### Comment by [Commenter7](https://www.reddit.com/r/subreddit2/comments/sub2/comment_7/) on 2023-02-04 00:00:00 *(in reply to a comment not included)*\n\n"
                "Comment 7 (orphaned)\n\n"
            )
        assert result == expected


def test_create_non_indented_md_from_thread():
    comment1 = {
        "id": "com1",
        "parent_id": None,
        "author": "Commenter1",
        "comment_body": "Comment 1 on Submission 1",
        "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/",
        "created_utc": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.PARENT,
    }
    comment2 = {
        "id": "com2",
        "parent_id": "com1",
        "author": "Commenter2",
        "comment_body": "Comment 2 (reply to Comment 1)",
        "permalink": "https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/",
        "created_utc": datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment1["children"].append(comment2)

    result = create_non_indented_md_from_thread(comment1)
    expected = (
        "### Comment by [Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/) on 2024-01-02 00:00:00\n\n"
        "Comment 1 on Submission 1\n\n"
        "#### Comment by [Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/) on 2024-01-03 00:00:00\n\n"
        "Comment 2 (reply to Comment 1)\n\n"
    )
    assert result == expected

    comment3 = {
        "id": "com3",
        "parent_id": None,
        "author": "Commenter3",
        "comment_body": "Comment 3 on Submission 2",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/",
        "created_utc": datetime.datetime(2023, 2, 2, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.PARENT,
    }
    comment4 = {
        "id": "com4",
        "parent_id": "com3",
        "author": "Commenter4",
        "comment_body": "Comment 4 (reply to Comment 3)\n\n# Header in comment",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/",
        "created_utc": datetime.datetime(2023, 2, 3, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment5 = {
        "id": "com5",
        "parent_id": "com4",
        "author": "Commenter5",
        "comment_body": "Comment 5 (reply to Comment 4)",
        "permalink": "https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/",
        "created_utc": datetime.datetime(2023, 2, 4, tzinfo=datetime.UTC).timestamp(),
        "children": [],
        "type": CommentType.REPLY,
    }
    comment3["children"].append(comment4)
    comment4["children"].append(comment5)

    result = create_non_indented_md_from_thread(comment3)
    expected = (
        "### Comment by [Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/) on 2023-02-02 00:00:00\n\n"
        "Comment 3 on Submission 2\n\n"
        "#### Comment by [Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/) on 2023-02-03 00:00:00\n\n"
        "Comment 4 (reply to Comment 3)\n\n\\# Header in comment\n\n"
        "##### Comment by [Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/) on 2023-02-04 00:00:00\n\n"
        "Comment 5 (reply to Comment 4)\n\n"
    )
    assert result == expected


def test_save_comments_to_markdown(in_memory_db: sqlite3.Connection, tmp_path: Path) -> None:
    # Use tmp_path fixture from pytest for temporary directories
    markdown_files_dir = tmp_path / "markdown"
    temp_files_dir = tmp_path / "temp"
    markdown_files_dir.mkdir()
    temp_files_dir.mkdir()

    save_comments_to_markdown(in_memory_db, str(markdown_files_dir), str(temp_files_dir))

    # Expected content for 2023 markdown file
    expected_2023_md = (
        "**r/subreddit2** | Posted by Author2 _2023-02-01 00:00:00_\n"
        "### [Submission Title 2 # Header](https://reddit.com/r/subreddit2/comments/sub2)\n\n"
        "Submission Body 2\n\n"
        "# Another Header\n\n"
        "- **[Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/)** _2023-02-02 00:00:00_:\n\n"
        "    Comment 3 on Submission 2\n"
        "    - **[Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/)** _2023-02-03 00:00:00_:\n\n"
        "        Comment 4 (reply to Comment 3)\n\n"
        "        # Header in comment\n"
        "        - **[Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/)** _2023-02-04 00:00:00_:\n\n"
        "            Comment 5 (reply to Comment 4)\n"
        "- **[Commenter7](https://www.reddit.com/r/subreddit2/comments/sub2/comment_7/)** _2023-02-04 00:00:00_ *(in reply to a comment not included)*:\n\n"
        "    Comment 7 (orphaned)\n"
        "\n---\n\n"
    )
    expected_2023_epub = (
        metablock_template.format(year=2023)
        + "## [Submission Title 2 # Header](https://reddit.com/r/subreddit2/comments/sub2)\n"
        "**Subreddit**: r/subreddit2 | **Posted by**: Author2 _2023-02-01 00:00:00_\n\n"
        "Submission Body 2\n\n\\# Another Header\n\n"
        "### Comment by [Commenter3](https://www.reddit.com/r/subreddit2/comments/sub2/comment_3/) on 2023-02-02 00:00:00\n\n"
        "Comment 3 on Submission 2\n\n"
        "#### Comment by [Commenter4](https://www.reddit.com/r/subreddit2/comments/sub2/comment_4/) on 2023-02-03 00:00:00\n\n"
        "Comment 4 (reply to Comment 3)\n\n\\# Header in comment\n\n"
        "##### Comment by [Commenter5](https://www.reddit.com/r/subreddit2/comments/sub2/comment_5/) on 2023-02-04 00:00:00\n\n"
        "Comment 5 (reply to Comment 4)\n\n"
        "### Comment by [Commenter7](https://www.reddit.com/r/subreddit2/comments/sub2/comment_7/) on 2023-02-04 00:00:00 *(in reply to a comment not included)*\n\n"
        "Comment 7 (orphaned)\n\n"
    )

    # Expected content for 2024 markdown file
    expected_2024_md = (
        "**r/subreddit1** | Posted by Author1 _2024-01-01 00:00:00_\n"
        "### [Submission Title 1](https://reddit.com/r/subreddit1/comments/sub1)\n\n"
        "Submission Body 1\n\n"
        "- **[Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/)** _2024-01-02 00:00:00_:\n\n"
        "    Comment 1 on Submission 1\n"
        "    - **[Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/)** _2024-01-03 00:00:00_:\n\n"
        "        Comment 2 (reply to Comment 1)\n"
        "\n---\n\n"
    )
    expected_2024_epub = (
        metablock_template.format(year=2024)
        + "## [Submission Title 1](https://reddit.com/r/subreddit1/comments/sub1)\n"
        "**Subreddit**: r/subreddit1 | **Posted by**: Author1 _2024-01-01 00:00:00_\n\n"
        "Submission Body 1\n\n"
        "### Comment by [Commenter1](https://www.reddit.com/r/subreddit1/comments/sub1/comment_1/) on 2024-01-02 00:00:00\n\n"
        "Comment 1 on Submission 1\n\n"
        "#### Comment by [Commenter2](https://www.reddit.com/r/subreddit1/comments/sub1/comment_2/) on 2024-01-03 00:00:00\n\n"
        "Comment 2 (reply to Comment 1)\n\n"
    )

    # Check that the files were created and their content
    markdown_file_2023 = markdown_files_dir / "ven_anigha_reddit_archive_2023.md"
    markdown_file_2024 = markdown_files_dir / "ven_anigha_reddit_archive_2024.md"
    epub_file_2023 = temp_files_dir / "ven_anigha_reddit_archive_2023.md"
    epub_file_2024 = temp_files_dir / "ven_anigha_reddit_archive_2024.md"

    assert markdown_file_2023.read_text(encoding="utf-8") == expected_2023_md
    assert markdown_file_2024.read_text(encoding="utf-8") == expected_2024_md
    assert epub_file_2023.read_text(encoding="utf-8") == expected_2023_epub
    assert epub_file_2024.read_text(encoding="utf-8") == expected_2024_epub
