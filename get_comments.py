import praw
import os
from dotenv import load_dotenv
from tqdm import tqdm
import sqlite3

# Load environment variables from .env file
load_dotenv()

# Set up your Reddit app credentials from environment variables
reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    user_agent=os.getenv('USER_AGENT')
)
LIMIT = int(os.getenv('LIMIT')) if os.getenv('LIMIT') else None

def fetch_user_comments(username, till_comment_id=None):  # None will get entire history
    user = reddit.redditor(username)
    comments_by_submission = {}
    try:
        for comment in tqdm(user.comments.new(limit=LIMIT)):
            submission_id = comment.submission.id
            if submission_id not in comments_by_submission:
                submission = comment.submission
                comments_by_submission[submission_id] = {
                    'title': submission.title,
                    'body': submission.selftext,
                    'author': submission.author.name,
                    'created_at': submission.created_utc,
                    'link': submission.url,
                    'subreddit': submission.subreddit_name_prefixed,
                    'comments': []
                }
            comments_by_submission[submission_id]['comments'].append(comment)
            if comment.id == till_comment_id:
                break
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return comments_by_submission

def format_comment(comment):
    formatted_comment = f"**Comment by /u/{comment.author}**:\n\n{comment.body}\n\n"
    return formatted_comment

def save_comments_to_markdown(comments_by_submission, filename='user_comments.md'):
    with open(filename, 'w', encoding='utf-8') as file:
        for submission_id, submission_data in comments_by_submission.items():
            file.write(f"# {submission_data['title']}\n\n")
            file.write(f"**Submission Body**:\n\n{submission_data['body']}\n\n")
            file.write("---\n\n")
            comments = sorted(submission_data['comments'], key=lambda x: x.created_utc)
            for comment in comments:
                if comment.is_root:
                    file.write(f"## Root Comment\n\n")
                    file.write(format_comment(comment))
                    file.write("\n\n---\n\n")
                else:
                    parent_comment = reddit.comment(comment.parent_id)
                    file.write(f"## Reply to /u/{parent_comment.author}\n\n")
                    file.write(format_comment(parent_comment))
                    file.write(format_comment(comment))
                    file.write("\n\n---\n\n")

def save_comments_to_db(comments):
    # Create a connection to the SQLite database
    conn = sqlite3.connect('reddit_comments.db')
    c = conn.cursor()

    # Create submissions table
    c.execute('''
    CREATE TABLE IF NOT EXISTS submissions (
        id TEXT PRIMARY KEY,
        title TEXT,
        body TEXT,
        author TEXT,
        created_at REAL,
        link TEXT,
        subreddit TEXT
    )
    ''')

    # Create comments table with additional fields and foreign key constraint
    c.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        submission_id TEXT,
        author TEXT,
        created_utc INTEGER,
        parent_id TEXT,
        permalink TEXT,
        comment_body TEXT,
        FOREIGN KEY (submission_id) REFERENCES submissions (id),
        FOREIGN KEY (parent_id) REFERENCES comments (id)
    )
    ''')

    # Insert data into submissions and comments tables
    for submission_id, submission_data in comments.items():
        # Upsert into submissions table
        c.execute('''
        INSERT INTO submissions (id, title, body, author, created_at, link, subreddit)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            body=excluded.body,
            author=excluded.author,
            created_at=excluded.created_at,
            link=excluded.link,
            subreddit=excluded.subreddit
        ''', (
            submission_id,
            submission_data['title'],
            submission_data['body'],
            submission_data['author'],
            submission_data['created_at'],
            submission_data['link'],
            submission_data['subreddit']
        ))

        # Upsert into comments table
        for comment in submission_data['comments']:
            if comment.is_root:
                parent_comment = None
            else:
                parent_id = comment.parent_id[3:] if not comment.is_root else None
                parent_comment = reddit.comment(parent_id)
            for comm in [comment, parent_comment]:
                if comm:
                    parent_id = comm.parent_id[3:] if not comm.is_root else None
                    c.execute('''
                    INSERT INTO comments (id, submission_id, author, created_utc, parent_id, permalink, comment_body)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        submission_id=excluded.submission_id,
                        author=excluded.author,
                        created_utc=excluded.created_utc,
                        parent_id=excluded.parent_id,
                        permalink=excluded.permalink,
                        comment_body=excluded.comment_body
                    ''', (
                        comm.id,
                        submission_id,
                        comm.author.name,
                        comm.created_utc,
                        parent_id,
                        "https://www.reddit.com" + comm.permalink,
                        comm.body
                    ))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def main():
    username = os.getenv('TARGET_USERNAME')  # Load target username from environment variable
    comments_by_submission = fetch_user_comments(username)
    
    if comments_by_submission:
        save_comments_to_markdown(comments_by_submission)
        save_comments_to_db(comments_by_submission)
        print(f"Markdown document and database created with comments organized by submission.")
    else:
        print("No comments found for the user.")

if __name__ == "__main__":
    main()