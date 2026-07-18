# Ven Anīgha Reddit Archive

Archives Reddit comments by [Venerable Anīgha](https://www.reddit.com/user/Bhikkhu_Anigha/comments/) and [Sister Medhini](https://www.reddit.com/user/Sister_Medhini/comments/).

For a consumer-friendly fork with PDF and EPUB downloads, see: https://github.com/f0lie/reddit-ven-anigha-archive

## How it works

1. **`fetch_comments.py`** — pulls comments (and parent/OP context) via the Reddit API into **`reddit_comments.db`**.
2. **`generate_archive.py`** — writes yearly Markdown under **`markdown_files/`** (`standard` and `full` with parent quotes).
3. Shared DB helpers live in **`db.py`**.

Both scripts run every Sunday at midnight UTC via GitHub Actions.

## Local usage

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Requires CLIENT_ID, CLIENT_SECRET, USER_AGENT in .env (or the environment)
python fetch_comments.py

python generate_archive.py --type standard
python generate_archive.py --type full
```

Optional env for fetch: `LIMIT`, `TILL_LAST_COMMENT` (`true`/`false`, default `true`).
