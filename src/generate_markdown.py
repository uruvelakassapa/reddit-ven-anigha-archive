from typing import Any
import datetime
import re
import shutil
import sqlite3
import subprocess
from pathlib import Path
from enum import Enum

metablock_template = """\
---
title: \"Ven Anīgha Reddit Archive {year}\"
author: \"Ven Anīgha\"
date: \"{year}\"
description: \"Reddit discussions by Ven Anīgha in {year}.\"
lang: en
mainfont: "Source Serif 4"
fontsize: 12pt
geometry: margin=1in
documentclass: book
pdf-engine: xelatex
toc: true
toc-depth: 2
---

# Ven Anīgha Reddit Archive {year}

"""


def save_comments_to_markdown(conn: sqlite3.Connection, markdown_files_dir, temp_files_dir) -> None:
    """
    Saves Reddit submissions and comments from a SQLite database to markdown files.

    Organizes submissions by year and generates separate markdown files for each year.
    Creates both indented and non-indented (for EPUB) markdown formats.

    Args:
        conn: An SQLite database connection.
        markdown_files_dir: The directory to save the indented markdown files.
        temp_files_dir: The directory to save the non-indented markdown files used for EPUB generation.
    """
    markdown_dir = Path(markdown_files_dir)
    temp_dir = Path(temp_files_dir)
    temp_dir.mkdir(exist_ok=True)

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            id, title, body, author, created_at, link, subreddit, updated_at
        FROM submissions
        ORDER BY created_at DESC
        """
    )
    submissions = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    # Group submissions by year
    submissions_by_year = {}
    for submission in submissions:
        submission_dict = dict(zip(column_names, submission))
        # Get the year from created_at timestamp
        submission_time = datetime.datetime.fromtimestamp(submission_dict["created_at"], datetime.UTC)
        year = submission_time.year
        submissions_by_year.setdefault(year, []).append(submission_dict)

    # Process submissions year by year
    for year, submissions_in_year in submissions_by_year.items():
        markdown_text = ""
        epub_text = metablock_template.format(year=year)
        for submission_dict in submissions_in_year:
            markdown_text += create_intended_md_from_submission(cursor, submission_dict)
            epub_text += create_non_indented_md_from_submission(cursor, submission_dict)

        markdown_file = markdown_dir / f"ven_anigha_reddit_archive_{year}.md"
        markdown_file.write_text(markdown_text, encoding="utf-8")
        epub_file = temp_dir / f"ven_anigha_reddit_archive_{year}.md"
        epub_file.write_text(epub_text, encoding="utf-8")
        print(f"Generated markdown file with indention for {year} at: {markdown_file}")
        print(f"Generated markdown file for EPUB creation for {year} at: {epub_file}")


class CommentType(Enum):
    """
    Enum representing the type of a comment within a thread.
    """

    # Comments directly reponding to the submission.
    PARENT = 1
    # Comments replying to another comment.
    REPLY = 2
    # Comments replying to another comment but does not appear in the database.
    ORPHAN = 3


def create_thread_dicts(comments: list[list[Any]]) -> list[dict[str, Any]]:
    """
    Converts a list of comment tuples into a list of nested dictionaries representing the thread structure.

    Args:
        comments: A list of tuples, each representing a comment with its attributes.

    Returns:
        A list of dictionaries, where each dictionary represents a comment thread,
        potentially nested with child comments under the 'children' key.
    """
    comments_dict = {}
    for comment in comments:
        comments_dict[comment[0]] = {
            "id": comment[0],
            "submission_id": comment[1],
            "author": comment[2],
            "created_utc": comment[3],
            "parent_id": comment[4],
            "permalink": comment[5],
            "comment_body": comment[6],
            "updated_at": comment[7],
            "children": [],
            "type": None,
        }

    # Build the nested structure
    top_level_comments = []
    orphan_comments = []

    for comment in comments_dict.values():
        parent_id = comment["parent_id"]
        # Populate children comments
        if parent_id and parent_id in comments_dict:
            comments_dict[parent_id]["children"].append(comment)

        # Root comments have no parents.
        if not parent_id:
            comment["type"] = CommentType.PARENT
            top_level_comments.append(comment)
        # Orphan comments have parent_ids not in the database.
        elif parent_id not in comments_dict:
            comment["type"] = CommentType.ORPHAN
            orphan_comments.append(comment)
        else:
            # Reply comment are not added because they will be traversed by the parent comment
            comment["type"] = CommentType.REPLY

    top_level_comments.sort(key=lambda x: x["created_utc"])
    orphan_comments.sort(key=lambda x: x["created_utc"])

    # Append all orphan_comments to the end so that comments with parents are seen first.
    return top_level_comments + orphan_comments


def create_intended_md_from_submission(cursor: sqlite3.Cursor, submission_dict: dict[str, str | float]) -> str:
    """
    Generates indented markdown text for a given Reddit submission and its comments.

    Args:
        cursor: An SQLite database cursor.
        submission_dict: A dictionary containing the submission data.

    Returns:
        A string containing the markdown representation of the submission and its comments,
        with comments indented according to their nesting level.
    """
    submission_time_str = datetime.datetime.fromtimestamp(submission_dict["created_at"], datetime.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    submission_md = (
        f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n"
    )
    submission_md += f"### [{submission_dict['title']}]({submission_dict['link']})\n\n"
    submission_md += f"{submission_dict['body']}\n\n"

    # Fetch and process comments for the submission
    cursor.execute(
        """
        SELECT
            id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at
        FROM comments
        WHERE submission_id = ?
        ORDER BY created_utc
        """,
        (submission_dict["id"],),
    )
    comments = cursor.fetchall()

    comments_dict = create_thread_dicts(comments)
    for comment in comments_dict:
        submission_md += create_intended_md_from_thread(comment)
    return submission_md + "\n---\n\n"


def create_intended_md_from_thread(comment: dict[str, Any], level=0) -> str:
    """
    Recursively generates indented markdown text for a comment thread.

    Args:
        comment: A dictionary representing a comment thread.
        level: The nesting level of the comment (default: 0).

    Returns:
        A string containing the markdown representation of the comment and its replies,
        with each comment indented according to its nesting level.
    """
    indent = "    " * level
    content = "\n".join(f"{indent + '    ' + line if line else ''}" for line in comment["comment_body"].splitlines())

    parent_info = ""
    if comment["type"] == CommentType.ORPHAN:
        parent_info = " *(in reply to a comment not included)*"

    comment_time = datetime.datetime.fromtimestamp(comment["created_utc"], datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    comment_title = f"**[{comment['author']}]({comment['permalink']})** _{comment_time}_{parent_info}"

    markdown = f"{indent}- {comment_title}:\n\n{content}\n"

    sorted_children = sorted(comment["children"], key=lambda x: x["created_utc"])
    for child in sorted_children:
        markdown += create_intended_md_from_thread(child, level + 1)
    return markdown


def sanitize_markdown_content(content: str) -> str:
    """
    Sanitizes user-generated content to prevent it from interfering with EPUB markdown structure.

    Currently, it escapes markdown headers by prefixing them with a backslash.

    Args:
        content: The string content to sanitize.

    Returns:
        The sanitized string content.
    """
    # Escape markdown headers by prefixing with a backslash
    sanitized_content = re.sub(r"^(#+)", r"\\\1", content, flags=re.MULTILINE)
    return sanitized_content


def create_non_indented_md_from_submission(cursor: sqlite3.Cursor, submission_dict: dict[str, str | float]) -> str:
    """
    Generates non-indented markdown text for a given Reddit submission and its comments, suitable for EPUB generation.

    Uses markdown headings to represent the structure instead of indentation.

    Args:
        cursor: An SQLite database cursor.
        submission_dict: A dictionary containing the submission data.

    Returns:
        A string containing the markdown representation of the submission and its comments,
        without indentation and using headings for structure.
    """
    submission_time_str = datetime.datetime.fromtimestamp(submission_dict["created_at"], datetime.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    submission_md = f"## [{submission_dict['title']}]({submission_dict['link']})\n"
    submission_md += f"**Subreddit**: {submission_dict['subreddit']} | **Posted by**: {submission_dict['author']} _{submission_time_str}_\n\n"
    submission_md += f"{sanitize_markdown_content(submission_dict['body'])}\n\n"

    # Fetch and process comments for the submission
    cursor.execute(
        """
        SELECT
            id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at
        FROM comments
        WHERE submission_id = ?
        ORDER BY created_utc
        """,
        (submission_dict["id"],),
    )
    comments = cursor.fetchall()

    comments_dict = create_thread_dicts(comments)
    for comment in comments_dict:
        submission_md += create_non_indented_md_from_thread(comment)
    return submission_md


def create_non_indented_md_from_thread(comment: dict[str, Any], level=0) -> str:
    """
    Recursively generates non-indented markdown text for a comment thread, suitable for EPUB generation.

    Uses markdown headings to represent the comment hierarchy.

    Args:
        comment: A dictionary representing a comment thread.
        level: The nesting level of the comment (default: 0).

    Returns:
        A string containing the markdown representation of the comment and its replies,
        without indentation and using headings for structure.
    """
    # Use level to determine heading level (e.g., ###, ####)
    heading_prefix = "#" * (level + 3)  # Start from ### for comments
    comment_time = datetime.datetime.fromtimestamp(comment["created_utc"], datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")

    parent_info = ""
    if comment["type"] == CommentType.ORPHAN:
        parent_info = " *(in reply to a comment not included)*"
    comment_title = (
        f"{heading_prefix} Comment by [{comment['author']}]({comment['permalink']}) on {comment_time}{parent_info}"
    )

    markdown = f"{comment_title}\n\n{sanitize_markdown_content(comment['comment_body'])}\n\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(comment["children"], key=lambda x: x["created_utc"])
    for child in sorted_children:
        markdown += create_non_indented_md_from_thread(child, level + 1)
    return markdown


def convert_to_epub_and_pdf(input_dir: Path, epub_dir: str, pdf_dir) -> None:
    """
    Converts markdown files in a directory to EPUB and PDF formats using pandoc.

    Args:
        input_dir: The directory containing the markdown files.
        epub_dir: The directory to save the generated EPUB files.
        pdf_dir: The directory to save the generated PDF files.
    """
    epub_dir, pdf_dir = Path(epub_dir), Path(pdf_dir)

    for file in input_dir.glob("*.md"):
        base_name = file.stem
        epub_path = epub_dir / f"{base_name}.epub"
        pdf_path = pdf_dir / f"{base_name}.pdf"
        try:
            subprocess.run(
                [
                    "pandoc",
                    str(file),
                    "-o",
                    str(epub_path),
                ],
                check=True,
            )
            subprocess.run(
                [
                    "pandoc",
                    str(file),
                    "-o",
                    str(pdf_path),
                    "--pdf-engine",
                    "xelatex",
                ],
                check=True,
            )
            print(f"Converted {file} into {epub_path} and {pdf_path}.")
        except subprocess.CalledProcessError as e:
            print(f"Pandoc failed to convert {file}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while converting {file}: {e}")


if __name__ == "__main__":
    temp_files_dir = Path("temp_files")
    temp_files_dir.mkdir(exist_ok=True)
    try:
        with sqlite3.connect("reddit_comments.db") as conn:
            save_comments_to_markdown(conn, "markdown_files", temp_files_dir)

        convert_to_epub_and_pdf(temp_files_dir, "epub_files", "pdf_files")
    finally:
        shutil.rmtree(temp_files_dir)
