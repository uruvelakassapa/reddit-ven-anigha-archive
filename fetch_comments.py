import praw
import os
from dotenv import load_dotenv
from tqdm import tqdm
import sqlite3
import datetime
import json

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
    # TODO make this list of users
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

        # Prepare comment data dictionary
        comment_data = {
            'id': comment.id,
            'author': comment.author.name if comment.author else '[deleted]',
            'created_utc': comment.created_utc,
            'permalink': f"https://www.reddit.com{comment.permalink}",
            'body': comment.body,
            'parent': None  # Initialize parent as None
        }

        # Fetch and add parent comment details if it's not a root comment
        if not comment.is_root:
            try:
                # Fetching the parent might involve an extra API call per comment
                parent = comment.parent()
                # Check if parent is a Comment or Submission (for top-level replies)
                if isinstance(parent, praw.models.Comment):
                    parent_author_obj = getattr(parent, 'author', None)
                    parent_data = {
                        'id': parent.id,
                        'author': parent_author_obj.name if parent_author_obj else '[deleted]',
                        'body': getattr(parent, 'body', '[unavailable]'),
                        'permalink': f"https://www.reddit.com{getattr(parent, 'permalink', '')}"
                    }
                    comment_data['parent'] = parent_data
                # else: parent is likely the Submission, handled by submission_id already
            except Exception as e:
                print(f"Warning: Could not fetch parent for comment {comment.id}. Error: {e}")
                # Keep parent as None or add error info if needed

        comments_by_submission[submission_id]['comments'].append(comment_data) # Append dict instead of object

    return comments_by_submission

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

    # Create comments table with updated_at field and parent details
    c.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        submission_id TEXT,
        author TEXT,
        created_utc INTEGER,
        parent_id TEXT,
        permalink TEXT,
        comment_body TEXT,
        parent_author TEXT,
        parent_body TEXT,
        parent_permalink TEXT,
        updated_at REAL,
        FOREIGN KEY (submission_id) REFERENCES submissions (id)
        -- Note: We keep parent_id but don't enforce FK constraint on it directly here
        -- as the parent might not always be in our DB (e.g., if it wasn't fetched).
        -- FOREIGN KEY (parent_id) REFERENCES comments (id)
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
            datetime.datetime.now().timestamp()
        ))

        # Upsert into comments table using the comment_data dictionary
        for comment_data in submission_data['comments']:
            parent_info = comment_data.get('parent') # Get the parent dictionary
            parent_id = parent_info['id'] if parent_info else None
            parent_author = parent_info['author'] if parent_info else None
            parent_body = parent_info['body'] if parent_info else None
            parent_permalink = parent_info['permalink'] if parent_info else None

            c.execute('''
            INSERT INTO comments (id, submission_id, author, created_utc, parent_id, permalink, comment_body, parent_author, parent_body, parent_permalink, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                submission_id=excluded.submission_id,
                author=excluded.author,
                created_utc=excluded.created_utc,
                parent_id=excluded.parent_id,
                permalink=excluded.permalink,
                comment_body=excluded.comment_body,
                parent_author=excluded.parent_author,
                parent_body=excluded.parent_body,
                parent_permalink=excluded.parent_permalink,
                updated_at=excluded.updated_at
            ''', (
                comment_data['id'],
                submission_id,
                comment_data['author'],
                comment_data['created_utc'],
                parent_id,
                comment_data['permalink'],
                comment_data['body'],
                parent_author,
                parent_body,
                parent_permalink,
                datetime.datetime.now().timestamp()
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
    till_last_comment = json.loads(os.getenv('TILL_LAST_COMMENT', 'true').lower())

    if till_last_comment:
        last_comment_id = get_latest_comment_id()  # Get the latest comment id from the database
        comments_by_submission = fetch_user_comments(username, till_comment_id=last_comment_id)
    else:
        comments_by_submission = fetch_user_comments(username)

    if comments_by_submission:
        # Save the fetched comments as a JSON file
        with open(f"{username}_comments.json", 'w') as f:
            json.dump(comments_by_submission, f, indent=4)
        save_comments_to_db(comments_by_submission)
        print(f"Database updated with comments organized by submission.")
    else:
        print(f"No new comments found for {username}.")

if __name__ == "__main__":
    main()