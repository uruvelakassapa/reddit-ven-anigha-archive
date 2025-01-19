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


def save_comments_to_markdown(
    conn: sqlite3.Connection, markdown_files_dir, temp_files_dir
) -> None:
    markdown_dir = Path(markdown_files_dir)
    temp_dir = Path(temp_files_dir)
    temp_dir.mkdir(exist_ok=True)

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM submissions ORDER BY created_at DESC")
    submissions = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    # Group submissions by year
    submissions_by_year = {}
    for submission in submissions:
        submission_dict = dict(zip(column_names, submission))
        # Get the year from created_at timestamp
        submission_time = datetime.datetime.fromtimestamp(
            submission_dict["created_at"], datetime.UTC
        )
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
    # Comments directly reponding to the submission.
    PARENT = 1
    # Comments replying to another comment.
    REPLY = 2
    # Comments replying to another comment but does not appear in the database.
    ORPHAN = 3


def create_thread_dicts(threads: list[list[Any]]) -> list[dict[str, str]]:
    thread_dict = {}
    for thread in threads:
        thread_dict[thread[0]] = {
            "id": thread[0],
            "parent": thread[4],
            "user": thread[2],
            "content": thread[6],
            "url": thread[5],
            "created_at": thread[3],
            "children": [],
            "type": None,
        }

    # Build the nested structure
    top_level_threads = []
    orphan_threads = []

    for thread in thread_dict.values():
        parent_id = thread["parent"]
        # Populate children threads
        if parent_id and parent_id in thread_dict:
            thread_dict[parent_id]["children"].append(thread)

        # Root threads have no parents.
        if not parent_id:
            thread["type"] = CommentType.PARENT
            top_level_threads.append(thread)
        # Orphan threads have parent_ids not in the database.
        elif parent_id not in thread_dict:
            thread["type"] = CommentType.ORPHAN
            orphan_threads.append(thread)
        else:
            # Reply thread are not added because they will be traversed by the parent thread
            thread["type"] = CommentType.REPLY

    top_level_threads.sort(key=lambda x: x["created_at"])
    orphan_threads.sort(key=lambda x: x["created_at"])

    # Append all orphan_threads to the end so that comments with parents are seen first.
    return top_level_threads + orphan_threads


def create_intended_md_from_submission(
    cursor: sqlite3.Cursor, submission_dict: dict[str, str | float]
) -> str:
    submission_time_str = datetime.datetime.fromtimestamp(
        submission_dict["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")

    submission_md = f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n"
    submission_md += f"### [{submission_dict['title']}]({submission_dict['link']})\n\n"
    submission_md += f"{submission_dict['body']}\n\n"

    # Fetch and process comments for the submission
    cursor.execute(
        "SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc",
        (submission_dict["id"],),
    )
    comments = cursor.fetchall()

    threads = create_thread_dicts(comments)
    for thread in threads:
        submission_md += create_intended_md_from_thread(thread)
    return submission_md + "\n---\n\n"


def create_intended_md_from_thread(thread: dict[str, str], level=0) -> str:
    indent = "    " * level
    content = "\n".join(
        f"{indent + "    " + line if line else ""}"
        for line in thread["content"].splitlines()
    )

    parent_info = ""
    if thread["type"] == CommentType.ORPHAN:
        parent_info = " *(in reply to a comment not included)*"

    comment_time = datetime.datetime.fromtimestamp(
        thread["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")
    comment_title = (
        f"**[{thread['user']}]({thread['url']})** _{comment_time}_{parent_info}"
    )

    markdown = f"{indent}- {comment_title}:\n\n{content}\n"

    sorted_children = sorted(thread["children"], key=lambda x: x["created_at"])
    for child in sorted_children:
        markdown += create_intended_md_from_thread(child, level + 1)
    return markdown


def sanitize_markdown_content(content: str) -> str:
    """
    Sanitize user-generated content to prevent it from interfering with EPUB markdown structure.
    """
    # Escape markdown headers by prefixing with a backslash
    sanitized_content = re.sub(r"^(#+)", r"\\\1", content, flags=re.MULTILINE)
    return sanitized_content


def create_non_indented_md_from_submission(
    cursor: sqlite3.Cursor, submission_dict: dict[str, str | float]
) -> str:
    """
    Generate markdown for a submission without indentation for EPUB generation, using titles for structure.
    """
    submission_time_str = datetime.datetime.fromtimestamp(
        submission_dict["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")

    submission_md = f"## [{submission_dict['title']}]({submission_dict['link']})\n"
    submission_md += f"**Subreddit**: {submission_dict['subreddit']} | **Posted by**: {submission_dict['author']} _{submission_time_str}_\n\n"
    submission_md += f"{sanitize_markdown_content(submission_dict['body'])}\n\n"

    # Fetch and process comments for the submission
    cursor.execute(
        "SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc",
        (submission_dict["id"],),
    )
    comments = cursor.fetchall()

    threads = create_thread_dicts(comments)
    for thread in threads:
        submission_md += create_non_indented_md_from_thread(thread)
    return submission_md


def create_non_indented_md_from_thread(thread: dict[str, str], level=0) -> str:
    """
    Generate markdown for a thread without indentation for EPUB generation, using markdown headings.
    """
    # Use level to determine heading level (e.g., ###, ####)
    heading_prefix = "#" * (level + 3)  # Start from ### for comments
    comment_time = datetime.datetime.fromtimestamp(
        thread["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")

    parent_info = ""
    if thread["type"] == CommentType.ORPHAN:
        parent_info = " *(in reply to a comment not included)*"
    comment_title = f"{heading_prefix} Comment by [{thread['user']}]({thread['url']}) on {comment_time}{parent_info}"

    markdown = f"{comment_title}\n\n{sanitize_markdown_content(thread['content'])}\n\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(thread["children"], key=lambda x: x["created_at"])
    for child in sorted_children:
        markdown += create_non_indented_md_from_thread(child, level + 1)
    return markdown


def convert_to_epub_and_pdf(input_dir: Path, epub_dir: str, pdf_dir) -> None:
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
