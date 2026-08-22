"""Generate yearly Markdown archives from the SQLite database."""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from typing import Dict

import db
from threads import ThreadNode, build_comment_threads

MARKDOWN_DIR_DEFAULT = "markdown_files"

# Shared YAML/header; full mode adds Pandoc PDF-oriented fields.
_METABLOCK_COMMON = """\
---
title: "Ven Anīgha Reddit Archive {year}"
author: "Ven Anīgha"
date: "{year}"
description: "Reddit discussions by Ven Anīgha in {year}."
{extra}toc: true
toc-depth: 2
---

# Ven Anīgha Reddit Archive {year}

"""

_METABLOCK_FULL_EXTRA = """\
mainfont: "Source Serif 4"
fontsize: 12pt
geometry: margin=1in
documentclass: book
pdf-engine: xelatex
"""


def group_submissions_by_year(submissions) -> Dict[int, list]:
    by_year: Dict[int, list] = {}
    for sub in submissions:
        year = datetime.datetime.fromtimestamp(sub["created_at"], datetime.UTC).year
        by_year.setdefault(year, []).append(sub)
    return by_year


def format_timestamp(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _indent_block(text: str, indent: str) -> str:
    """Indent paragraphs as nested list content (not a Markdown blockquote)."""
    paragraphs = (text or "").split("\n\n")
    indented = [
        "\n".join(f"{indent}{line}" for line in paragraph.split("\n"))
        for paragraph in paragraphs
    ]
    return "\n\n".join(indented)


def _who_markdown(node: ThreadNode) -> str:
    if node.url and node.url != "#":
        return f"**[{node.user}]({node.url})**"
    return f"**{node.user}**"


def format_comment_markdown(node: ThreadNode, *, include_parents: bool, level: int) -> str:
    indent_str = "    " * level
    content_indent = "    " * (level + 1)
    indented_content = _indent_block(node.content, content_indent)

    who = _who_markdown(node)
    if node.synthetic:
        comment_title = who
    else:
        comment_title = f"{who} _{format_timestamp(node.created_at)}_"

    extra = ""
    if not include_parents and not node.synthetic and level == 0 and node.parent_id:
        if node.parent_user:
            extra = f" *(in reply to {node.parent_user})*"
        else:
            extra = " *(in reply to a comment not included)*"

    markdown = f"{indent_str}- {comment_title}{extra}:\n\n{indented_content}\n"

    for child in node.children:
        markdown += format_comment_markdown(
            child, include_parents=include_parents, level=level + 1
        )
    return markdown


def generate_submission_markdown(conn, submission, *, include_parents: bool) -> str:
    time_str = format_timestamp(submission["created_at"])
    md = f"**{submission['subreddit']}** | Posted by {submission['author']} _{time_str}_\n"
    md += f"### [{submission['title']}]({submission['link']})\n\n"
    md += f"{submission['body']}\n\n"

    comments = db.fetch_comments_for_submission(conn, submission["id"])
    if comments:
        for root in build_comment_threads(
            comments, include_missing_parents=include_parents
        ):
            md += format_comment_markdown(root, include_parents=include_parents, level=0)

    return md + "\n---\n\n"


def metablock_for(year: int, *, include_parents: bool) -> str:
    extra = _METABLOCK_FULL_EXTRA if include_parents else ""
    return _METABLOCK_COMMON.format(year=year, extra=extra)


def write_markdown_files(conn, output_type: str, md_dir: str) -> None:
    include_parents = output_type == "full"
    print(f"Generating {output_type} Markdown files...")
    os.makedirs(md_dir, exist_ok=True)

    submissions_by_year = group_submissions_by_year(db.fetch_submissions(conn))
    suffix = "_full" if include_parents else ""

    for year, submissions_in_year in submissions_by_year.items():
        path = os.path.join(md_dir, f"ven_anigha_reddit_archive{suffix}_{year}.md")
        print(f"  Writing {path}...")
        try:
            with open(path, "w", encoding="utf-8") as md_file:
                md_file.write(metablock_for(year, include_parents=include_parents))
                for submission in submissions_in_year:
                    md_file.write(
                        generate_submission_markdown(
                            conn, submission, include_parents=include_parents
                        )
                    )
            print(f"  Finished {path}")
        except OSError as e:
            print(f"  Error writing file {path}: {e}", file=sys.stderr)

    print(f"Finished generating {output_type} Markdown files.")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Reddit archive Markdown files.")
    parser.add_argument(
        "--type",
        choices=["standard", "full"],
        default="standard",
        help="Type of Markdown output ('standard' or 'full' with parent comments).",
    )
    parser.add_argument(
        "--db",
        default=db.DB_DEFAULT,
        help=f"Path to the SQLite database file (default: {db.DB_DEFAULT}).",
    )
    parser.add_argument(
        "--md-dir",
        default=MARKDOWN_DIR_DEFAULT,
        help=f"Directory to save Markdown files (default: {MARKDOWN_DIR_DEFAULT}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    conn = db.connect(args.db, must_exist=True)
    if not conn:
        return 1

    print(f"Connected to database: {args.db}")
    try:
        write_markdown_files(conn, args.type, args.md_dir)
    except Exception as e:
        print(f"\nAn unexpected error occurred during processing: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
        print("Database connection closed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
