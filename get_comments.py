import praw
import os
from dotenv import load_dotenv
from tqdm import tqdm
import sqlite3

import datetime
# Load environment variables from .env file
load_dotenv()

# Set up your Reddit app credentials from environment variables
reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    user_agent=os.getenv('USER_AGENT')
)

conn = sqlite3.connect('reddit_comments.db')

LIMIT = int(os.getenv('LIMIT')) if os.getenv('LIMIT') else None

def fetch_user_comments(username, till_comment_id=None):  # None will get entire history
    user = reddit.redditor(username)
    comments_by_submission = {}

    for comment in tqdm(user.comments.new(limit=LIMIT)):
        if comment.id == till_comment_id:
            break
        submission_id = comment.submission.id
        if submission_id not in comments_by_submission:
            submission = comment.submission
            comments_by_submission[submission_id] = {
                'title': submission.title,
                'body': submission.selftext,
                'author': submission.author.name if submission.author else None,
                'created_at': submission.created_utc,
                'link': submission.url,
                'subreddit': submission.subreddit_name_prefixed,
                'comments': []
            }
        comments_by_submission[submission_id]['comments'].append(comment)


    return comments_by_submission

def format_comment(comment):
    formatted_comment = f"**Comment by /u/{comment.author}**:\n\n{comment.body}\n\n"
    return formatted_comment

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
            # file.write("---\n\n")
            c.execute('SELECT * FROM comments WHERE submission_id = ? ORDER BY created_utc', (submission_dict['id'],))
            comments = c.fetchall()
            # Get column names for comments
            create_nested_structure(comments, file)
  


def create_nested_structure(threads, file_obj):
    
    thread_dict = {}
    for thread in threads:
        thread_dict[thread[0]] = {
            "parent": thread[4],
            "user": thread[2],
            "content": thread[6],  # Replace '\n' with a markdown line break
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
        # Split content by paragraphs and handle indentation
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

def save_comments_to_db(comments):
    c = conn.cursor()

    # Create submissions table with updated_at field
    c.execute('''
    CREATE TABLE IF NOT EXISTS submissions (
        id TEXT PRIMARY KEY,
        title TEXT,
        body TEXT,
        author TEXT,
        created_at REAL,
        link TEXT,
        subreddit TEXT,
        updated_at REAL
    )
    ''')

    # Create comments table with updated_at field
    c.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        submission_id TEXT,
        author TEXT,
        created_utc INTEGER,
        parent_id TEXT,
        permalink TEXT,
        comment_body TEXT,
        updated_at REAL,
        FOREIGN KEY (submission_id) REFERENCES submissions (id),
        FOREIGN KEY (parent_id) REFERENCES comments (id)
    )
    ''')

    # Insert data into submissions and comments tables
    for submission_id, submission_data in comments.items():
        # Upsert into submissions table
        c.execute('''
        INSERT INTO submissions (id, title, body, author, created_at, link, subreddit, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            body=excluded.body,
            author=excluded.author,
            created_at=excluded.created_at,
            link=excluded.link,
            subreddit=excluded.subreddit,
            updated_at=excluded.updated_at
        ''', (
            submission_id,
            submission_data['title'],
            submission_data['body'],
            submission_data['author'],
            submission_data['created_at'],
            submission_data['link'],
            submission_data['subreddit'],
            datetime.datetime.now().timestamp()  # Update the updated_at field with the current timestamp
        ))

        # Upsert into comments table
        for comment in submission_data['comments']:
            parent_id = comment.parent_id[3:] if not comment.is_root else None
            c.execute('''
            INSERT INTO comments (id, submission_id, author, created_utc, parent_id, permalink, comment_body, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                submission_id=excluded.submission_id,
                author=excluded.author,
                created_utc=excluded.created_utc,
                parent_id=excluded.parent_id,
                permalink=excluded.permalink,
                comment_body=excluded.comment_body,
                updated_at=excluded.updated_at
            ''', (
                comment.id,
                submission_id,
                comment.author.name if comment.author else None,
                comment.created_utc,
                parent_id,
                "https://www.reddit.com" + comment.permalink,
                comment.body,
                datetime.datetime.now().timestamp()  # Update the updated_at field with the current timestamp
            ))

    # Commit the changes to the database
    conn.commit()


def get_latest_comment_id():
    try:
        c = conn.cursor()
        c.execute('SELECT id FROM comments ORDER BY created_utc DESC LIMIT 1')
        latest_comment = c.fetchone()
        return latest_comment[0] if latest_comment else None
    except sqlite3.OperationalError:
        # This will catch errors like 'no such table: comments'
        return None

def main():
    username = os.getenv('TARGET_USERNAME')  # Load target username from environment variable
    till_last_comment = os.getenv('TILL_LAST_COMMENT')

    if till_last_comment:
        last_comment_id = get_latest_comment_id()  # Get the latest comment id from the database
        comments_by_submission = fetch_user_comments(username, till_comment_id=last_comment_id)
    else:
        comments_by_submission = fetch_user_comments(username)

    if comments_by_submission:
        save_comments_to_db(comments_by_submission)
        print(f"Markdown document and database created with comments organized by submission.")
        save_comments_to_markdown() 

    else:
        print(f"No new comments found for {username}.")


if __name__ == "__main__":
    main()