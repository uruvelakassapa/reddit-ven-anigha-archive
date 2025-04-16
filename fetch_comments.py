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

def fetch_user_comments(username, till_comment_id=None):  # Fetches comments for a single user
    print(f"Fetching comments for user: {username}")
    user = reddit.redditor(username)
    comments_by_submission = {}

    for comment in tqdm(user.comments.new(limit=LIMIT), desc=f"User {username}"):
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

def get_latest_comment_id_for_user(username):
    """Fetches the ID of the latest comment stored in the DB for a specific user."""
    try:
        c = conn.cursor()
        # Query for the latest comment ID specifically for the given author
        c.execute('SELECT id FROM comments WHERE author = ? ORDER BY created_utc DESC LIMIT 1', (username,))
        latest_comment = c.fetchone()
        return latest_comment[0] if latest_comment else None
    except sqlite3.OperationalError:
        # This handles cases where the table might not exist yet
        print("Warning: Comments table not found or query failed. Fetching all comments for user.")
        return None

def main():
    usernames_str = os.getenv('TARGET_USERNAMES')  # Load target usernames from environment variable
    if not usernames_str:
        print("Error: TARGET_USERNAMES environment variable not set or empty.")
        return
    usernames = [name.strip() for name in usernames_str.split(',')] # Split into list

    till_last_comment = json.loads(os.getenv('TILL_LAST_COMMENT', 'true').lower())

    all_comments_by_submission = {}
    print(f"Processing users: {', '.join(usernames)}")
    for username in usernames:
        last_comment_id_for_user = None
        if till_last_comment:
            last_comment_id_for_user = get_latest_comment_id_for_user(username) # Get latest comment ID for *this* user
            if last_comment_id_for_user:
                print(f"Fetching comments for {username} newer than comment ID: {last_comment_id_for_user}")
            else:
                print(f"No previous comments found for {username} in DB. Fetching all available comments.")
        else:
            print(f"Fetching all available comments for {username} (TILL_LAST_COMMENT is false).")

        try:
            user_comments = fetch_user_comments(username, till_comment_id=last_comment_id_for_user)
            # Merge results - simple update works if submission IDs are unique across users' comments lists
            # If a submission could theoretically contain comments from multiple target users being fetched,
            # you might need a more sophisticated merge (e.g., merging the 'comments' lists within each submission)
            # For now, a simple update assumes submission context is tied to the user being fetched.
            all_comments_by_submission.update(user_comments)
        except Exception as e:
            print(f"Error fetching comments for user {username}: {e}")
            # Decide if you want to stop or continue with other users
            # continue

    if all_comments_by_submission:
        save_comments_to_db(all_comments_by_submission)
        print(f"Database updated with comments for users: {', '.join(usernames)}.")
    else:
        print(f"No new comments found for users: {', '.join(usernames)}.")

if __name__ == "__main__":
    main()