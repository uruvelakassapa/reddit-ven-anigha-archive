"""Generate yearly Markdown archives from the SQLite database."""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import db

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


@dataclass
class ThreadNode:
    id: str
    parent_id: Optional[str]
    user: str
    content: str
    url: str
    created_at: float
    parent_user: Optional[str] = None
    parent_content: Optional[str] = None
    children: List["ThreadNode"] = field(default_factory=list)


def group_submissions_by_year(submissions) -> Dict[int, list]:
    by_year: Dict[int, list] = {}
    for sub in submissions:
        year = datetime.datetime.fromtimestamp(sub["created_at"], datetime.UTC).year
        by_year.setdefault(year, []).append(sub)
    return by_year


def build_comment_threads(comments) -> List[ThreadNode]:
    """Build nested threads from a flat comment list; return ordered roots."""
    nodes: Dict[str, ThreadNode] = {}
    for row in comments:
        nodes[row["id"]] = ThreadNode(
            id=row["id"],
            parent_id=row["parent_id"],
            user=row["author"],
            content=row["comment_body"],
            url=row["permalink"],
            created_at=row["created_utc"],
            parent_user=row["parent_author"],
            parent_content=row["parent_body"],
        )

    top_level: List[ThreadNode] = []
    orphans: List[ThreadNode] = []
    for node in nodes.values():
        parent_id = node.parent_id
        if parent_id and parent_id in nodes:
            nodes[parent_id].children.append(node)
        elif not parent_id:
            top_level.append(node)
        else:
            orphans.append(node)

    top_level.sort(key=lambda n: n.created_at)
    return top_level + orphans


def format_timestamp(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _indent_block(text: str, indent: str, *, quote: bool = False) -> str:
    """Indent paragraphs; optionally as a Markdown blockquote."""
    prefix = f"{indent}> " if quote else indent
    blank = f"{indent}>" if quote else ""
    paragraphs = text.split("\n\n")
    indented = [
        "\n".join(f"{prefix}{line}" for line in paragraph.split("\n"))
        for paragraph in paragraphs
    ]
    joiner = f"\n{blank}\n" if quote else "\n\n"
    return joiner.join(indented)


def format_parent_info_full(node: ThreadNode, indent: str) -> str:
    if not node.parent_id:
        return ""
    if node.parent_user and node.parent_content is not None:
        quoted = _indent_block(node.parent_content, indent, quote=True)
        return f"\n\n{indent}*(In reply to {node.parent_user}):*\n{quoted}\n"
    return f"\n\n{indent}*(In reply to a comment not available)*\n"


def format_comment_markdown(node: ThreadNode, *, include_parents: bool, level: int) -> str:
    indent_str = "    " * level
    content_indent = "    " * (level + 1)
    indented_content = _indent_block(node.content, content_indent)

    parent_info_md = ""
    if include_parents:
        parent_info_md = format_parent_info_full(node, content_indent)
    elif level > 0 and node.parent_id:
        parent_info_md = " *(in reply to a comment not included)*"

    comment_time = format_timestamp(node.created_at)
    title_base = f"**[{node.user}]({node.url})** _{comment_time}_"

    if not include_parents and parent_info_md:
        comment_title = f"{title_base}{parent_info_md}"
        parent_info_md = ""
    else:
        comment_title = title_base

    if include_parents:
        markdown = f"{indent_str}- {comment_title}:{parent_info_md}\n{indented_content}\n"
    else:
        markdown = f"{indent_str}- {comment_title}:\n\n{indented_content}\n"

    for child in sorted(node.children, key=lambda n: n.created_at):
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
        for root in build_comment_threads(comments):
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
