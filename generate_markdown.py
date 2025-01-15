from typing import Any
import datetime
import os
import re
import shutil
import sqlite3
import subprocess

metablock_template = """\
---
title: \"Ven Anīgha Reddit Archive {year}\"
author: \"Ven Anīgha\"
date: \"{year}\"
description: \"Reddit discussions by Ven Anīgha in {year}.\"
mainfont: "Times New Roman"
fontsize: 12pt
geometry: margin=1in
documentclass: book
pdf-engine: xelatex
toc: true
toc-depth: 2
---

# Ven Anīgha Reddit Archive {year}

"""

def save_comments_to_markdown():
    c.execute('SELECT * FROM submissions ORDER BY created_at DESC')
    submissions = c.fetchall()
    column_names = [description[0] for description in c.description]

    # Group submissions by year
    submissions_by_year = {}
    for submission in submissions:
        submission_dict = dict(zip(column_names, submission))
        # Get the year from created_at timestamp
        submission_time = datetime.datetime.fromtimestamp(submission_dict['created_at'], datetime.UTC)
        year = submission_time.year
        submissions_by_year.setdefault(year, []).append(submission_dict)

    # Process submissions year by year
    for year, submissions_in_year in submissions_by_year.items():
        md_filename = f'markdown_files/ven_anigha_reddit_archive_{year}.md'
        epub_md_filename = f'temp_files/ven_anigha_reddit_archive_{year}.md'

        metablock = metablock_template.format(year=year)
        with open(epub_md_filename, 'w', encoding='utf-8') as epub_md_file:
            with open(md_filename, 'w', encoding='utf-8') as md_file:
                epub_md_file.write(metablock)

                for submission_dict in submissions_in_year:
                    md_file.write(create_intended_md_from_submission(submission_dict))
                    epub_md_file.write(create_non_indented_md_from_submission(submission_dict))

                print(f"Markdown files with indention generated for {year}: {md_filename}")
                print(f"Markdown files for EPUB generated for {year}: {md_filename}")
        

def create_thread_dicts(threads: list[list[Any]]) -> list[dict[str,str]]:
    thread_dict = {}
    for thread in threads:
        thread_dict[thread[0]] = {
            "id": thread[0],
            "parent": thread[4],
            "user": thread[2],
            "content": thread[6],
            "url": thread[5],
            "created_at": thread[3],
            "children": []
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
    top_level_threads.sort(key=lambda x: x['created_at'])

    return top_level_threads + orphan_threads

def create_intended_md_from_submission(submission_dict: dict[str, str | float]) -> str:
    submission_time_str = datetime.datetime.fromtimestamp(submission_dict['created_at'], datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')

    submission_md = f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n"
    submission_md += f"### [{submission_dict['title']}]({submission_dict["link"]})\n\n"
    submission_md += f"{submission_dict['body']}\n\n"

    # Fetch and process comments for the submission
    c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
    comments = c.fetchall()

    threads = create_thread_dicts(comments)
    for thread in threads:
        submission_md += create_intended_md_from_thread(thread)
    return submission_md + "\n---\n\n"


def create_intended_md_from_thread(thread: dict[str, str], level=0) -> str:
    indent_str = '    ' * level 
    content_indent_str = '    ' * (level + 1)
    paragraphs = thread['content'].split('\n\n')
    indented_paragraphs = ['\n'.join(f"{content_indent_str}{line}" for line in paragraph.split('\n')) for paragraph in paragraphs]
    indented_content = '\n\n'.join(indented_paragraphs)
    
    parent_info = ""
    if thread['parent']:
        parent_info = " *(in reply to a comment not included)*"

    comment_time = datetime.datetime.fromtimestamp(thread['created_at'], datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')
    comment_title = f"**[{thread['user']}]({thread['url']})** _{comment_time}{parent_info}"

    markdown = f"{indent_str}- {comment_title}:\n\n{indented_content}\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(thread["children"], key=lambda x: x['created_at'])
    for child in sorted_children:
        markdown += create_intended_md_from_thread(child, level + 1)
    return markdown

def sanitize_markdown_content(content: str) -> str:
    """
    Sanitize user-generated content to prevent it from interfering with EPUB markdown structure.
    """
    # Escape markdown headers by prefixing with a backslash
    sanitized_content = re.sub(r'^(#+)', r'\\\1', content, flags=re.MULTILINE)
    return sanitized_content


def create_non_indented_md_from_submission(submission_dict: dict[str, str | float]) -> str:
    """
    Generate markdown for a submission without indentation for EPUB generation, using titles for structure.
    """
    submission_time_str = datetime.datetime.fromtimestamp(submission_dict['created_at'], datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')

    submission_md = f"## [{submission_dict['title']}]({submission_dict["link"]})\n"
    submission_md += f"**Subreddit**: {submission_dict['subreddit']} | **Posted by**: {submission_dict['author']} _{submission_time_str}_\n\n"
    submission_md += f"{sanitize_markdown_content(submission_dict['body'])}\n\n"

    # Fetch and process comments for the submission
    c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
    comments = c.fetchall()

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
    comment_time = datetime.datetime.fromtimestamp(thread['created_at'], datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')

    parent_info = ""
    if thread['parent']:
        parent_info = " *(in reply to a comment not included)*"
    comment_title = f"{heading_prefix} Comment by [{thread['user']}]({thread['url']}) on {comment_time}{parent_info}"

    markdown = f"{comment_title}\n\n{sanitize_markdown_content(thread['content'])}\n\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(thread["children"], key=lambda x: x['created_at'])
    for child in sorted_children:
        markdown += create_non_indented_md_from_thread(child, level + 1)
    return markdown

def convert_to_epub_and_pdf(input_dir: str, epub_dir: str, pdf_dir) -> None:
    for file in os.listdir(input_dir):
        if file.endswith(".md"):
            base_name = os.path.splitext(file)[0]
            try:
                subprocess.run(["pandoc", os.path.join(input_dir, file), "-o", os.path.join(epub_dir, f"{base_name}.epub")])
                subprocess.run(["pandoc", os.path.join(input_dir, file), "-o", os.path.join(pdf_dir, f"{base_name}.pdf"), "--pdf-engine", "xelatex"])
                print(f"Converted {file} to EPUB and PDF.")
            except Exception as e:
                print(f"Failed to convert {file}: {e}")

if __name__ == "__main__":
    conn = sqlite3.connect('reddit_comments.db')
    c = conn.cursor()

    os.makedirs('temp_files', exist_ok=True)
    save_comments_to_markdown()
    print("Markdown file generated from the database.")
    convert_to_epub_and_pdf("temp_files", "epub_files", "pdf_files")
    shutil.rmtree("temp_files")