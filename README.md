# Ven Anīgha Reddit Archive

Archives Reddit comments by [Venerable Anīgha](https://www.reddit.com/user/Bhikkhu_Anigha/comments/) and [Sister Medhini](https://www.reddit.com/user/Sister_Medhini/comments/).

For another consumer-friendly fork with PDF/EPUB packaging history, see: https://github.com/f0lie/reddit-ven-anigha-archive

## Outputs

| Format | Location | Notes |
|--------|----------|--------|
| SQLite | `reddit_comments.db` | Source of truth |
| Markdown | `markdown_files/` | Yearly `standard` and `full` |
| EPUB | `epub/` | From full Markdown via Pandoc |
| Web site | `docs/` | Browse, search, SuttaCentral links |

Enable **GitHub Pages** from the `docs/` folder on `main` to publish the site.

## How it works

1. **`fetch_comments.py`** — Reddit API → `reddit_comments.db` (preserves text if Reddit later shows `[deleted]`).
2. **`generate_archive.py`** — yearly Markdown (`standard` / `full`).
3. **`generate_epub.py`** — full Markdown → EPUB (needs [Pandoc](https://pandoc.org/)).
4. **`generate_site.py`** — static HTML under `docs/` (search + HTML-only sutta links).
5. Shared DB helpers in **`db.py`**.

Weekly GitHub Action (Sunday 00:00 UTC): fetch → Markdown → EPUB → site → commit.

## Local usage

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # set CLIENT_ID, CLIENT_SECRET, USER_AGENT

python fetch_comments.py
python generate_archive.py --type standard
python generate_archive.py --type full
python generate_epub.py      # requires pandoc
python generate_site.py

# Preview the site
python -m http.server 8000 --directory docs
# open http://127.0.0.1:8000/
```

Optional fetch env: `LIMIT`, `TILL_LAST_COMMENT` (`true`/`false`, default `true`).
