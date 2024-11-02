import sqlite3
import datetime

conn = sqlite3.connect('reddit_comments.db')

def save_comments_to_markdown(filename='ven_anigha_reddit_archive.md'):
    c = conn.cursor()
    with open(filename, 'w', encoding='utf-8') as file:
        c.execute('SELECT * FROM submissions ORDER BY created_at DESC')
        submissions = c.fetchall()
        column_names = [description[0] for description in c.description]
        for submission in submissions:
            submission_dict = dict(zip(column_names, submission))

            file.write(f"**{submission_dict['subreddit']}** | Posted by {submission_dict['author']} _{datetime.datetime.fromtimestamp(submission_dict['created_at']).strftime('%Y-%m-%d %H:%M:%S')}_\n")
            file.write(f"### {submission_dict['title']}\n\n")
            file.write(f"{submission_dict['body']}\n\n")
            c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
            comments = c.fetchall()
            create_nested_structure(comments, file)

def create_nested_structure(threads, file_obj):
    thread_dict = {}
    for thread in threads:
        thread_dict[thread[0]] = {
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
            thread_dict[parent_id]["children"].append(thread_dict[thread_id])

    # Function to recursively create the markdown structure
    def generate_markdown(thread, indent=0):
        indent_str = '    ' * indent
        content_indent_str = '    ' * (indent + 1)
        paragraphs = thread['content'].split('\n\n')
        indented_paragraphs = ['\n'.join(f"{content_indent_str}{line}" for line in paragraph.split('\n')) for paragraph in paragraphs]
        indented_content = '\n\n'.join(indented_paragraphs)
        markdown = f"{indent_str}- **[{thread['user']}]({thread['url']})** _{datetime.datetime.fromtimestamp(thread['created_at']).strftime('%Y-%m-%d %H:%M:%S')}_:\n\n{indented_content}\n"
        for child in thread["children"]:
            markdown += generate_markdown(child, indent + 1)
        return markdown

    # Find the root threads (those without a parent)
    roots = [thread for thread_id, thread in thread_dict.items() if not thread["parent"]]

    # Generate markdown for each root thread
    markdown_output = ""
    for root in roots:
        markdown_output += generate_markdown(root)
    markdown_output += "\n---\n\n"
    # Write the output to the file object
    file_obj.write(markdown_output)

def main():
    save_comments_to_markdown()
    print("Markdown file generated from the database.")

if __name__ == "__main__":
    main()