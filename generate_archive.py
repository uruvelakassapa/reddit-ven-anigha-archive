# generate_archive.py
import argparse
import datetime
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

# --- Constants ---

MARKDOWN_DIR_DEFAULT = "markdown_files"
PDF_DIR_DEFAULT = "pdf_files"
DB_DEFAULT = "reddit_comments.db"

METABLOCK_TEMPLATE_STANDARD = """\
---
title: "Ven Anīgha Reddit Archive {year}"
author: "Ven Anīgha"
date: "{year}"
description: "Reddit discussions by Ven Anīgha in {year}."
toc: true
toc-depth: 2
---

# Ven Anīgha Reddit Archive {year}

"""

METABLOCK_TEMPLATE_FULL = """\
---
title: "Ven Anīgha Reddit Archive {year}"
author: "Ven Anīgha"
date: "{year}"
description: "Reddit discussions by Ven Anīgha in {year}."
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

# --- Database Interaction ---

def connect_db(db_path: str) -> Optional[sqlite3.Connection]:
    """Connects to the SQLite database."""
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}", file=sys.stderr)
        return None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}", file=sys.stderr)
        return None

def fetch_submissions(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    """Fetches all submissions ordered by creation date."""
    cursor.execute("SELECT * FROM submissions ORDER BY created_at DESC")
    submissions_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in submissions_data]

def fetch_comments_for_submission(cursor: sqlite3.Cursor, submission_id: str, fetch_full: bool) -> List[Tuple]:
    """Fetches comments for a given submission ID."""
    if fetch_full:
        cursor.execute(
            """SELECT id, author, created_utc, parent_id, permalink, comment_body,
                      parent_author, parent_body
               FROM comments
               WHERE submission_id = ? ORDER BY created_utc""",
            (submission_id,),
        )
    else:
        cursor.execute(
            """SELECT id, author, created_utc, parent_id, permalink, comment_body
               FROM comments
               WHERE submission_id = ? ORDER BY created_utc""",
            (submission_id,),
        )
    return cursor.fetchall()

# --- Data Structuring ---

def group_submissions_by_year(submissions: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """Groups submissions into a dictionary keyed by year."""
    submissions_by_year = {}
    for sub in submissions:
        sub_time = datetime.datetime.fromtimestamp(sub["created_at"], datetime.UTC)
        year = sub_time.year
        submissions_by_year.setdefault(year, []).append(sub)
    return submissions_by_year

def build_comment_thread_structure(comments: List[Tuple], column_names: List[str]) -> Tuple[Dict[str, Dict], List[Dict]]:
    """Builds a nested thread structure from a flat list of comments."""
    thread_dict = {}
    for comment_tuple in comments:
        comment = dict(zip(column_names, comment_tuple))
        thread_dict[comment["id"]] = {
            "id": comment["id"],
            "parent": comment.get("parent_id"),
            "user": comment["author"],
            "content": comment["comment_body"],
            "url": comment["permalink"],
            "created_at": comment["created_utc"],
            "parent_user": comment.get("parent_author"), # None if not fetched
            "parent_content": comment.get("parent_body"), # None if not fetched
            "children": [],
        }

    top_level_threads = []
    orphan_threads = []
    for thread in thread_dict.values():
        parent_id = thread["parent"]
        if parent_id and parent_id in thread_dict:
            thread_dict[parent_id]["children"].append(thread)
        elif not parent_id:
            top_level_threads.append(thread)
        else: # Has parent_id but parent not in this batch
            orphan_threads.append(thread)

    top_level_threads.sort(key=lambda x: x["created_at"])
    # Orphans are appended after sorted top-level comments
    ordered_roots = top_level_threads + orphan_threads
    return thread_dict, ordered_roots

# --- Markdown Generation ---

def format_timestamp(timestamp: float) -> str:
    """Formats a Unix timestamp into a human-readable string."""
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")

def format_parent_info_full(thread: Dict[str, Any], indent: str) -> str:
    """Formats the parent comment information for the 'full' output."""
    parent_user = thread.get("parent_user")
    parent_content = thread.get("parent_content")

    if not thread.get("parent"): # Not a reply
        return ""
    if parent_user and parent_content is not None:
        parent_paragraphs = parent_content.split("\n\n")
        indented_parent_paragraphs = [
            "\n".join(f"{indent}> {line}" for line in paragraph.split("\n"))
            for paragraph in parent_paragraphs
        ]
        indented_parent_content = f"\n{indent}>\n".join(indented_parent_paragraphs)
        return f"\n\n{indent}*(In reply to {parent_user}):*\n{indented_parent_content}\n"
    else:
        return f"\n\n{indent}*(In reply to a comment not available)*\n"

def format_comment_markdown(thread: Dict[str, Any], output_type: str, level: int) -> str:
    """Formats a single comment (and its children recursively) into Markdown."""
    indent_str = "    " * level
    content_indent_str = "    " * (level + 1)

    # Format main comment content
    paragraphs = thread["content"].split("\n\n")
    indented_paragraphs = [
        "\n".join(f"{content_indent_str}{line}" for line in paragraph.split("\n"))
        for paragraph in paragraphs
    ]
    indented_content = "\n\n".join(indented_paragraphs)

    # Format parent info (if applicable)
    parent_info_md = ""
    if output_type == 'full':
        parent_info_md = format_parent_info_full(thread, content_indent_str)
    elif level > 0 and thread["parent"]: # Basic check for standard mode
         # A more robust check would involve passing the full thread dict down,
         # but keeping it simpler based on the original logic.
         parent_info_md = " *(in reply to a comment not included)*" # Added directly to title below

    # Format comment title/header
    comment_time_str = format_timestamp(thread["created_at"])
    comment_title_base = f"**[{thread['user']}]({thread['url']})** _{comment_time_str}_"

    if output_type == 'standard' and parent_info_md:
        comment_title = f"{comment_title_base}{parent_info_md}"
        parent_info_md = "" # Clear it as it's now part of the title
    else:
        comment_title = comment_title_base

    # Assemble the comment markdown
    if output_type == 'full':
        markdown = f"{indent_str}- {comment_title}:{parent_info_md}\n{indented_content}\n"
    else: # standard
        markdown = f"{indent_str}- {comment_title}:\n\n{indented_content}\n"


    # Recursively format children
    sorted_children = sorted(thread["children"], key=lambda x: x["created_at"])
    for child in sorted_children:
        markdown += format_comment_markdown(child, output_type, level + 1)

    return markdown

def generate_submission_markdown(cursor: sqlite3.Cursor, submission: Dict[str, Any], output_type: str) -> str:
    """Generates the Markdown for a single submission, including its comments."""
    submission_time_str = format_timestamp(submission["created_at"])
    fetch_full = (output_type == 'full')

    md = f"**{submission['subreddit']}** | Posted by {submission['author']} _{submission_time_str}_\n"
    md += f"### [{submission['title']}]({submission['link']})\n\n"
    md += f"{submission['body']}\n\n"

    comments_tuples = fetch_comments_for_submission(cursor, submission["id"], fetch_full)
    if comments_tuples:
        comment_column_names = [desc[0] for desc in cursor.description]
        _, ordered_roots = build_comment_thread_structure(comments_tuples, comment_column_names)
        for root_thread in ordered_roots:
            md += format_comment_markdown(root_thread, output_type, level=0)

    return md + "\n---\n\n"

def write_markdown_files(cursor: sqlite3.Cursor, output_type: str, md_dir: str):
    """Generates and writes all Markdown files, grouped by year."""
    print(f"Generating {output_type} Markdown files...")
    os.makedirs(md_dir, exist_ok=True)

    submissions = fetch_submissions(cursor)
    submissions_by_year = group_submissions_by_year(submissions)

    metablock_template = METABLOCK_TEMPLATE_FULL if output_type == 'full' else METABLOCK_TEMPLATE_STANDARD

    for year, submissions_in_year in submissions_by_year.items():
        filename_suffix = "_full" if output_type == 'full' else ""
        md_filename = os.path.join(md_dir, f"ven_anigha_reddit_archive{filename_suffix}_{year}.md")
        metablock = metablock_template.format(year=year)

        print(f"  Writing {md_filename}...")
        try:
            with open(md_filename, "w", encoding="utf-8") as md_file:
                md_file.write(metablock)
                for submission in submissions_in_year:
                    md_file.write(generate_submission_markdown(cursor, submission, output_type))
            print(f"  Finished {md_filename}")
        except IOError as e:
            print(f"  Error writing file {md_filename}: {e}", file=sys.stderr)

    print(f"Finished generating {output_type} Markdown files.")

# --- Main Execution ---

def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Generate Reddit archive Markdown files.")
    parser.add_argument(
        "--type",
        choices=['standard', 'full'],
        default='standard',
        help="Type of Markdown output ('standard' or 'full' with parent comments)."
    )
    parser.add_argument(
        "--db",
        default=DB_DEFAULT,
        help=f"Path to the SQLite database file (default: {DB_DEFAULT})."
    )
    parser.add_argument(
        "--md-dir",
        default=MARKDOWN_DIR_DEFAULT,
        help=f"Directory to save Markdown files (default: {MARKDOWN_DIR_DEFAULT})."
    )
    return parser.parse_args()

def main() -> int:
    """Main function to orchestrate the archive generation."""
    args = parse_arguments()

    conn = connect_db(args.db)
    if not conn:
        return 1

    exit_code = 0
    try:
        cursor = conn.cursor()
        write_markdown_files(cursor, args.type, args.md_dir)
    except Exception as e:
        print(f"\nAn unexpected error occurred during processing: {e}", file=sys.stderr)
        exit_code = 1
    finally:
        conn.close()
        print("Database connection closed.")

    return exit_code

if __name__ == "__main__":
    sys.exit(main())