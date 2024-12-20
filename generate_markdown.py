import sqlite3
import datetime

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
        submission_time = datetime.datetime.utcfromtimestamp(submission_dict['created_at'])
        year = submission_time.year
        submissions_by_year.setdefault(year, []).append(submission_dict)

    # Process submissions year by year
    for year, submissions_in_year in submissions_by_year.items():
        filename = f'ven_anigha_reddit_archive_{year}.md'
        with open(filename, 'w', encoding='utf-8') as file:
            for submission_dict in submissions_in_year:
                # Format submission time
                submission_time_str = datetime.datetime.utcfromtimestamp(submission_dict['created_at']).strftime('%Y-%m-%d %H:%M:%S')

                file.write(f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{submission_time_str}_\n")
                file.write(f"### {submission_dict['title']}\n\n")
                file.write(f"{submission_dict['body']}\n\n")

                # Fetch and process comments for the submission
                c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
                comments = c.fetchall()
                create_nested_structure(comments, file)
            print(f"Markdown file generated for {year}: {filename}")

def create_nested_structure(threads, file_obj):
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
    for thread_id, thread in thread_dict.items():
        parent_id = thread["parent"]
        if parent_id and parent_id in thread_dict:
            thread_dict[parent_id]["children"].append(thread)
        else:
            thread["parent_missing"] = parent_id

    # Separate roots into top-level comments and orphan comments
    top_level_roots = [thread for thread in thread_dict.values() if not thread["parent"]]
    orphan_roots = [thread for thread in thread_dict.values() if thread.get("parent_missing") and thread["parent"]]

    # Sort top-level roots by 'created_at' ascending
    top_level_roots.sort(key=lambda x: x['created_at'])

    markdown_output = ""

    # Process top-level roots first
    for root in top_level_roots:
        markdown_output += generate_markdown(root)

    # Optionally process orphan roots
    for root in orphan_roots:
        markdown_output += generate_markdown(root)

    markdown_output += "\n---\n\n"
    file_obj.write(markdown_output)

def generate_markdown(thread, indent=0):
    indent_str = '    ' * indent
    content_indent_str = '    ' * (indent + 1)
    paragraphs = thread['content'].split('\n\n')
    indented_paragraphs = ['\n'.join(f"{content_indent_str}{line}" for line in paragraph.split('\n')) for paragraph in paragraphs]
    indented_content = '\n\n'.join(indented_paragraphs)
    
    parent_info = ""
    if thread['parent']:
        parent_info = " *(in reply to a comment not included)*"

    comment_time = datetime.datetime.utcfromtimestamp(thread['created_at']).strftime('%Y-%m-%d %H:%M:%S')
    
    markdown = f"{indent_str}- **[{thread['user']}]({thread['url']})** _{comment_time}{parent_info}:\n\n{indented_content}\n"

    # Sort children by 'created_at' ascending
    sorted_children = sorted(thread["children"], key=lambda x: x['created_at'])
    for child in sorted_children:
        markdown += generate_markdown(child, indent + 1)
    return markdown

def main():
    save_comments_to_markdown()
    print("Markdown file generated from the database.")

if __name__ == "__main__":
    main()