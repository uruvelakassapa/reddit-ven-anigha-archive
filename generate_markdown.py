import sqlite3
import datetime

metablock_template = """\
---
title: \"Ven Anīgha Reddit Archive {year}\"
author: \"Ven Anīgha\"
date: \"{year}\"
description: \"Reddit discussions by Ven Anīgha in {year}.\"
---

# Ven Anīgha Reddit Archive {year}

"""

conn = sqlite3.connect('reddit_comments.db')

def save_comments_to_markdown():
    c = conn.cursor()
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
        filename = f'markdown_files/ven_anigha_reddit_archive_{year}.md'
        with open(filename, 'w', encoding='utf-8') as file:
            metablock = metablock_template.format(year=year)
            file.write(metablock)

            for submission_dict in submissions_in_year:
                # Format submission time
                submission_time_str = datetime.datetime.fromtimestamp(submission_dict['created_at'], datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')

                file.write(f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n")
                file.write(f"### {submission_dict['title']}\n\n")
                file.write(f"{submission_dict['body']}\n\n")

                # Fetch and process comments for the submission
                c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
                comments = c.fetchall()

                threads = create_thread_dicts(comments)
                for thread in threads:
                    file.write(generate_markdown(thread))
                file.write("\n---\n\n")

            print(f"Markdown file generated for {year}: {filename}")

def create_thread_dicts(threads: list[list[str]]) -> list[dict[str,str]]:
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


def generate_markdown(thread: dict[str, str], level=0) -> str:
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
        markdown += generate_markdown(child, level + 1)
    return markdown

def main():
    save_comments_to_markdown()
    print("Markdown file generated from the database.")

if __name__ == "__main__":
    main()