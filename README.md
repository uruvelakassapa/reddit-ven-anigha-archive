# Ven Anīgha Reddit Archive

Archives Reddit comments by [Venerable Anīgha](https://www.reddit.com/user/Bhikkhu_Anigha/comments/) and [Sister Medhini](https://www.reddit.com/user/Sister_Medhini/comments/).

For another consumer-friendly fork with PDF/EPUB packaging history, see: https://github.com/f0lie/reddit-ven-anigha-archive

## How it works

1. **`fetch_comments.py`** — pulls comments (and parent/OP context) via the Reddit API into **`reddit_comments.db`**.
2. **`generate_archive.py`** — writes yearly Markdown under **`markdown_files/`** (`standard` and `full` with parent quotes).
3. **`generate_epub.py`** — converts full yearly Markdown to **`epub/`** via [Pandoc](https://pandoc.org/).
4. Shared DB helpers live in **`db.py`**.

The weekly GitHub Action (Sunday 00:00 UTC) runs fetch → Markdown → EPUB and commits updates.

## Local usage

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy and fill credentials (see .env.example)
cp .env.example .env

python fetch_comments.py

python generate_archive.py --type standard
python generate_archive.py --type full

# Requires pandoc on PATH (e.g. brew install pandoc)
python generate_epub.py
```

Optional env for fetch: `LIMIT`, `TILL_LAST_COMMENT` (`true`/`false`, default `true`).

### EPUB only for some years

```bash
python generate_epub.py --year 2026
python generate_epub.py --year 2024 --year 2025
```
