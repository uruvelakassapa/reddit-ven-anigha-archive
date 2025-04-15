from typing import Any
import datetime
import os
import sqlite3

metablock_template = """\
---
title: \"Ven Anīgha Reddit Archive {year}\"
author: \"Ven Anīgha\"
date: \"{year}\"
description: \"Reddit discussions by Ven Anīgha in {year}.\"
toc: true
toc-depth: 2
---

# Ven Anīgha Reddit Archive {year}

"""


def save_comments_to_markdown(cursor: sqlite3.Cursor):
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
        md_filename = f"markdown_files/ven_anigha_reddit_archive_{year}.md"
        metablock = metablock_template.format(year=year)
        with open(md_filename, "w", encoding="utf-8") as md_file:
            md_file.write(
                metablock
            )  # Write metablock to the main markdown file

            for submission_dict in submissions_in_year:
                md_file.write(
                    create_intended_md_from_submission(cursor, submission_dict)
                )

                print(
                    f"Markdown files with indention generated for {year}: {md_filename}"
                )


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
            top_level_threads.append(thread)
        # Orphan threads have parent_ids not in the database.
        elif parent_id not in thread_dict:
            orphan_threads.append(thread)

    # Sort top-level roots by 'created_at' ascending
    top_level_threads.sort(key=lambda x: x["created_at"])

    return top_level_threads + orphan_threads


def create_intended_md_from_submission(
    cursor: sqlite3.Cursor, submission_dict: dict[str, Any]
) -> str:
    submission_time_str = datetime.datetime.fromtimestamp(
        submission_dict["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")

    submission_md = f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n"
    submission_md += (
        f"### [{submission_dict['title']}]({submission_dict['link']})\n\n"
    )
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


def create_intended_md_from_thread(thread: dict[str, Any], level=0) -> str:
    indent_str = "    " * level
    content_indent_str = "    " * (level + 1)
    paragraphs = thread["content"].split("\n\n")
    indented_paragraphs = [
        "\n".join(
            f"{content_indent_str}{line}" for line in paragraph.split("\n")
        )
        for paragraph in paragraphs
    ]
    indented_content = "\n\n".join(indented_paragraphs)

    parent_info = ""
    if thread["parent"]:
        parent_info = " *(in reply to a comment not included)*"

    comment_time = datetime.datetime.fromtimestamp(
        thread["created_at"], datetime.UTC
    ).strftime("%Y-%m-%d %H:%M:%S")
    comment_title = (
        f"**[{thread['user']}]({thread['url']})** _{comment_time}{parent_info}"
    )

    markdown = f"{indent_str}- {comment_title}:\n\n{indented_content}\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(thread["children"], key=lambda x: x["created_at"])
    for child in sorted_children:
        markdown += create_intended_md_from_thread(child, level + 1)
    return markdown


if __name__ == "__main__":
    conn = None  # Initialize conn to None
    try:
        conn = sqlite3.connect("reddit_comments.db")
        c = conn.cursor()

        # Ensure markdown output directory exists
        os.makedirs("markdown_files", exist_ok=True)

        save_comments_to_markdown(c)
        print("Markdown files generated from the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
