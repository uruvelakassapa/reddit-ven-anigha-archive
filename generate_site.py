"""Generate a static HTML site from the SQLite archive (web viewer)."""

from __future__ import annotations

import argparse
import datetime
import html
import json
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import db

SITE_DIR_DEFAULT = "docs"
EPUB_DIR_DEFAULT = "epub"
TEACHERS = frozenset({"Bhikkhu_Anigha", "Sister_Medhini"})

# Conservative sutta refs only (web viewer enrichment — not applied to MD/EPUB).
_SUTTA_RE = re.compile(r"\b(DN|MN|SN|AN)\s*(\d+(?:\.\d+)?)\b", re.IGNORECASE)

_CSS = """\
:root {
  --bg: #f7f4ef;
  --fg: #1a1a1a;
  --muted: #5c5c5c;
  --card: #fffefb;
  --border: #e0d8cc;
  --accent: #3d5a40;
  --teacher: #eef5ee;
  --op: #f3f0ea;
  --link: #1a4d8c;
  --max: 44rem;
  font-family: "Source Serif 4", "Iowan Old Style", Palatino, Georgia, serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--fg);
  line-height: 1.55;
}
a { color: var(--link); }
header.site {
  background: var(--accent);
  color: #f7f4ef;
  padding: 1rem 1.25rem;
}
header.site a { color: #e8f0e8; }
header.site .inner, main, footer.site .inner {
  max-width: var(--max);
  margin: 0 auto;
}
header.site h1 { margin: 0 0 0.25rem; font-size: 1.35rem; font-weight: 600; }
header.site p { margin: 0; opacity: 0.9; font-size: 0.95rem; }
nav.crumbs { font-size: 0.9rem; margin: 1rem 0; color: var(--muted); }
main { padding: 0 1.25rem 3rem; }
footer.site {
  border-top: 1px solid var(--border);
  padding: 1.25rem;
  color: var(--muted);
  font-size: 0.9rem;
}
.card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1rem 1.15rem;
  margin: 0.75rem 0;
}
.card h2, .card h3 { margin-top: 0; }
.meta { color: var(--muted); font-size: 0.9rem; }
.list { list-style: none; padding: 0; margin: 0; }
.list li { border-bottom: 1px solid var(--border); padding: 0.75rem 0; }
.list li:last-child { border-bottom: none; }
.list a.title { font-weight: 600; text-decoration: none; }
.list a.title:hover { text-decoration: underline; }
.search-box {
  display: flex; gap: 0.5rem; flex-wrap: wrap;
  margin: 1rem 0 1.5rem;
}
.search-box input {
  flex: 1 1 14rem;
  padding: 0.55rem 0.75rem;
  border: 1px solid var(--border);
  border-radius: 6px;
  font: inherit;
  background: var(--card);
}
.search-box button, .chip {
  padding: 0.55rem 0.9rem;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--card);
  font: inherit;
  cursor: pointer;
}
.search-box button { background: var(--accent); color: #fff; border-color: var(--accent); }
#search-status { color: var(--muted); font-size: 0.9rem; margin-bottom: 0.5rem; }
#search-results .list li { background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
.op {
  background: var(--op);
  border-left: 4px solid var(--border);
  padding: 0.85rem 1rem;
  margin: 1rem 0;
  border-radius: 0 6px 6px 0;
}
.comment {
  margin: 1rem 0 1rem 0;
  padding: 0.85rem 1rem;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--card);
}
.comment.teacher {
  background: var(--teacher);
  border-color: #c5d6c7;
}
.comment .who { font-weight: 600; }
.comment .when { color: var(--muted); font-size: 0.88rem; margin-left: 0.35rem; }
.comment .body { margin-top: 0.6rem; white-space: pre-wrap; }
.comment .body p { margin: 0 0 0.75rem; }
.comment .body p:last-child { margin-bottom: 0; }
.parent-quote {
  margin: 0.6rem 0 0.85rem;
  padding: 0.5rem 0.75rem;
  border-left: 3px solid #cbbfae;
  color: var(--muted);
  font-size: 0.95rem;
  white-space: pre-wrap;
}
.children { margin-left: 1rem; padding-left: 0.5rem; border-left: 2px solid var(--border); }
.epub-list { display: flex; flex-wrap: wrap; gap: 0.5rem; }
.epub-list a {
  display: inline-block;
  padding: 0.4rem 0.75rem;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 6px;
  text-decoration: none;
}
.epub-list a:hover { border-color: var(--accent); }
"""

_SEARCH_JS = """\
(function () {
  const input = document.getElementById("q");
  const status = document.getElementById("search-status");
  const results = document.getElementById("search-results");
  if (!input || !results) return;

  let index = null;
  let loading = null;

  async function loadIndex() {
    if (index) return index;
    if (loading) return loading;
    status.textContent = "Loading search index…";
    loading = fetch("search-index.json")
      .then((r) => r.json())
      .then((data) => {
        index = data;
        status.textContent = "";
        return index;
      })
      .catch(() => {
        status.textContent = "Could not load search index.";
        loading = null;
        return [];
      });
    return loading;
  }

  function render(hits) {
    if (!hits.length) {
      results.innerHTML = "<p class=\\"meta\\">No matching threads.</p>";
      return;
    }
    const ul = document.createElement("ul");
    ul.className = "list";
    for (const h of hits.slice(0, 50)) {
      const li = document.createElement("li");
      const authors = (h.teachers || []).join(", ") || "—";
      li.innerHTML =
        "<a class=\\"title\\" href=\\"" + h.url + "\\">" + escapeHtml(h.title) + "</a>" +
        "<div class=\\"meta\\">" + escapeHtml(h.subreddit || "") +
        " · " + escapeHtml(String(h.year)) +
        " · " + escapeHtml(authors) + "</div>";
      ul.appendChild(li);
    }
    results.innerHTML = "";
    results.appendChild(ul);
    status.textContent = hits.length + " result(s)" + (hits.length > 50 ? " (showing 50)" : "");
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function search(q) {
    q = q.trim().toLowerCase();
    if (!q) {
      results.innerHTML = "";
      status.textContent = "";
      return;
    }
    const terms = q.split(/\\s+/).filter(Boolean);
    const hits = index.filter((item) => {
      const hay = item.text;
      return terms.every((t) => hay.includes(t));
    });
    render(hits);
  }

  let timer = null;
  input.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(async () => {
      await loadIndex();
      search(input.value);
    }, 150);
  });

  const params = new URLSearchParams(location.search);
  if (params.get("q")) {
    input.value = params.get("q");
    loadIndex().then(() => search(input.value));
  }
})();
"""


@dataclass
class ThreadNode:
    id: str
    parent_id: Optional[str]
    user: str
    content: str
    url: str
    created_at: float
    parent_user: Optional[str] = None
    parent_content: Optional[str] = None
    children: List["ThreadNode"] = field(default_factory=list)


def format_timestamp(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def group_submissions_by_year(submissions) -> Dict[int, list]:
    by_year: Dict[int, list] = {}
    for sub in submissions:
        year = datetime.datetime.fromtimestamp(sub["created_at"], datetime.UTC).year
        by_year.setdefault(year, []).append(sub)
    return by_year


def build_comment_threads(comments) -> List[ThreadNode]:
    nodes: Dict[str, ThreadNode] = {}
    for row in comments:
        nodes[row["id"]] = ThreadNode(
            id=row["id"],
            parent_id=row["parent_id"],
            user=row["author"] or "[deleted]",
            content=row["comment_body"] or "",
            url=row["permalink"] or "#",
            created_at=row["created_utc"] or 0,
            parent_user=row["parent_author"],
            parent_content=row["parent_body"],
        )

    top_level: List[ThreadNode] = []
    orphans: List[ThreadNode] = []
    for node in nodes.values():
        if node.parent_id and node.parent_id in nodes:
            nodes[node.parent_id].children.append(node)
        elif not node.parent_id:
            top_level.append(node)
        else:
            orphans.append(node)

    top_level.sort(key=lambda n: n.created_at)
    return top_level + orphans


def linkify_suttas(text: str) -> str:
    """Escape HTML, then turn DN/MN/SN/AN citations into SuttaCentral links."""
    escaped = html.escape(text)

    def repl(match: re.Match) -> str:
        collection = match.group(1).upper()
        number = match.group(2)
        slug = f"{collection.lower()}{number}"
        label = f"{collection} {number}"
        href = f"https://suttacentral.net/{slug}"
        return f'<a href="{href}" rel="noopener noreferrer" target="_blank">{label}</a>'

    return _SUTTA_RE.sub(repl, escaped)


def body_to_html(text: str) -> str:
    """Paragraph-ish HTML with sutta links."""
    text = text or ""
    parts = re.split(r"\n\s*\n", text)
    chunks = []
    for part in parts:
        part = part.strip("\n")
        if not part:
            continue
        # Keep single newlines visible inside a paragraph
        linked = linkify_suttas(part).replace("\n", "<br>\n")
        chunks.append(f"<p>{linked}</p>")
    return "\n".join(chunks) if chunks else "<p></p>"


def page_shell(
    title: str,
    body: str,
    *,
    root: str = "",
    description: str = "Archive of Reddit discussions with Ven. Anīgha and Sister Medhini.",
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <meta name="description" content="{html.escape(description)}">
  <link rel="stylesheet" href="{root}assets/style.css">
</head>
<body>
  <header class="site">
    <div class="inner">
      <h1><a href="{root}index.html" style="color:inherit;text-decoration:none">Ven Anīgha Reddit Archive</a></h1>
      <p>Comments by Bhikkhu Anīgha &amp; Sister Medhini</p>
    </div>
  </header>
  <main>
{body}
  </main>
  <footer class="site">
    <div class="inner">
      Generated from the SQLite archive. Sutta links (SuttaCentral) appear on this site only.
    </div>
  </footer>
</body>
</html>
"""


def teachers_in_comments(comments) -> List[str]:
    seen = []
    for row in comments:
        author = row["author"]
        if author in TEACHERS and author not in seen:
            seen.append(author)
    return seen


def render_comment(node: ThreadNode) -> str:
    classes = "comment teacher" if node.user in TEACHERS else "comment"
    who = html.escape(node.user)
    when = html.escape(format_timestamp(node.created_at))
    url = html.escape(node.url, quote=True)

    parent_html = ""
    if node.parent_id and node.parent_user and node.parent_content is not None:
        parent_html = (
            f'<div class="parent-quote"><strong>In reply to '
            f"{html.escape(node.parent_user)}:</strong><br>"
            f"{body_to_html(node.parent_content)}</div>"
        )
    elif node.parent_id:
        parent_html = (
            '<div class="parent-quote"><em>In reply to a comment not available</em></div>'
        )

    children_html = ""
    if node.children:
        kids = "\n".join(
            render_comment(c) for c in sorted(node.children, key=lambda n: n.created_at)
        )
        children_html = f'<div class="children">\n{kids}\n</div>'

    return f"""<article class="{classes}" id="c-{html.escape(node.id)}">
  <div><a class="who" href="{url}">{who}</a><span class="when">{when}</span></div>
  {parent_html}
  <div class="body">{body_to_html(node.content)}</div>
  {children_html}
</article>"""


def render_thread_page(sub, comments, *, year: int) -> str:
    roots = build_comment_threads(comments)
    comments_html = "\n".join(render_comment(r) for r in roots) or "<p class=\"meta\">No comments archived.</p>"
    title = sub["title"] or "(untitled)"
    author = sub["author"] or "[deleted]"
    subreddit = sub["subreddit"] or ""
    when = format_timestamp(sub["created_at"])
    link = sub["link"] or "#"

    body = f"""    <nav class="crumbs"><a href="../index.html">Home</a> · <a href="../year/{year}.html">{year}</a></nav>
    <article>
      <p class="meta">{html.escape(subreddit)} · Posted by {html.escape(str(author))} · {html.escape(when)}</p>
      <h2>{html.escape(title)}</h2>
      <p class="meta"><a href="{html.escape(link, quote=True)}">Original on Reddit</a></p>
      <div class="op">{body_to_html(sub["body"] or "")}</div>
      <h3>Comments</h3>
      {comments_html}
    </article>"""
    return page_shell(f"{title} · {year}", body, root="../", description=title)


def render_year_page(year: int, items: Sequence[dict]) -> str:
    lis = []
    for item in items:
        lis.append(
            f"""<li>
  <a class="title" href="../thread/{html.escape(item['id'])}.html">{html.escape(item['title'])}</a>
  <div class="meta">{html.escape(item['subreddit'])} · {html.escape(item['date'])}
  · {html.escape(', '.join(item['teachers']) or '—')}</div>
</li>"""
        )
    body = f"""    <nav class="crumbs"><a href="../index.html">Home</a> · {year}</nav>
    <h2>{year}</h2>
    <p class="meta">{len(items)} discussion(s)</p>
    <ul class="list">
{chr(10).join(lis)}
    </ul>"""
    return page_shell(f"Archive {year}", body, root="../")


def render_home(years: Sequence[int], epub_years: Sequence[int], thread_count: int) -> str:
    year_links = "\n".join(
        f'      <li><a class="title" href="year/{y}.html">{y}</a></li>' for y in years
    )
    epub_links = "\n".join(
        f'      <a href="epub/ven_anigha_reddit_archive_{y}.epub">{y} EPUB</a>'
        for y in epub_years
    ) or "      <span class=\"meta\">No EPUBs found.</span>"

    body = f"""    <section class="card">
      <h2>About</h2>
      <p>This site presents archived Reddit Q&amp;A involving
      <strong>Bhikkhu Anīgha</strong> and <strong>Sister Medhini</strong>
      (chiefly r/HillsideHermitage). Thread pages include full context;
      citations like MN 44 are linked to SuttaCentral on this site only.</p>
      <p class="meta">{thread_count} threads across {len(years)} year(s).</p>
    </section>

    <section class="card">
      <h2>Search</h2>
      <div class="search-box">
        <input id="q" type="search" placeholder="Search titles and discussion text…" autocomplete="off">
      </div>
      <div id="search-status"></div>
      <div id="search-results"></div>
    </section>

    <section class="card">
      <h2>Browse by year</h2>
      <ul class="list">
{year_links}
      </ul>
    </section>

    <section class="card">
      <h2>Download EPUB</h2>
      <div class="epub-list">
{epub_links}
      </div>
    </section>
    <script src="assets/search.js"></script>"""
    return page_shell("Ven Anīgha Reddit Archive", body, root="")


def write_assets(site_dir: Path) -> None:
    assets = site_dir / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    (assets / "style.css").write_text(_CSS, encoding="utf-8")
    (assets / "search.js").write_text(_SEARCH_JS, encoding="utf-8")


def copy_epubs(epub_dir: Path, site_dir: Path) -> List[int]:
    dest = site_dir / "epub"
    dest.mkdir(parents=True, exist_ok=True)
    years: List[int] = []
    if not epub_dir.is_dir():
        return years
    for path in sorted(epub_dir.glob("ven_anigha_reddit_archive_*.epub")):
        match = re.search(r"_(\d{4})\.epub$", path.name)
        if not match:
            continue
        shutil.copy2(path, dest / path.name)
        years.append(int(match.group(1)))
    return years


def generate_site(conn, site_dir: Path, epub_dir: Path) -> None:
    if site_dir.exists():
        # Remove generated content but keep site_dir if it's docs with other stuff —
        # for a dedicated output tree, clear known subdirs/files we own.
        for name in ("assets", "year", "thread", "epub", "index.html", "search-index.json"):
            path = site_dir / name
            if path.is_dir():
                shutil.rmtree(path)
            elif path.is_file():
                path.unlink()
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "year").mkdir()
    (site_dir / "thread").mkdir()
    write_assets(site_dir)
    epub_years = copy_epubs(epub_dir, site_dir)

    submissions = db.fetch_submissions(conn)
    by_year = group_submissions_by_year(submissions)
    search_index = []
    year_summaries: Dict[int, List[dict]] = {}

    print(f"Generating site into {site_dir}/ …")
    for year in sorted(by_year.keys(), reverse=True):
        year_items = []
        for sub in by_year[year]:
            comments = db.fetch_comments_for_submission(conn, sub["id"])
            teachers = teachers_in_comments(comments)
            date = format_timestamp(sub["created_at"])
            title = sub["title"] or "(untitled)"
            thread_path = f"thread/{sub['id']}.html"

            page = render_thread_page(sub, comments, year=year)
            (site_dir / "thread" / f"{sub['id']}.html").write_text(page, encoding="utf-8")

            # Search corpus: title + OP + teacher comments
            text_parts = [title, sub["body"] or "", sub["subreddit"] or ""]
            for row in comments:
                if row["author"] in TEACHERS:
                    text_parts.append(row["comment_body"] or "")
            hay = " ".join(text_parts).lower()

            search_index.append(
                {
                    "id": sub["id"],
                    "title": title,
                    "year": year,
                    "subreddit": sub["subreddit"] or "",
                    "teachers": teachers,
                    "url": thread_path,
                    "text": hay,
                }
            )
            year_items.append(
                {
                    "id": sub["id"],
                    "title": title,
                    "subreddit": sub["subreddit"] or "",
                    "date": date,
                    "teachers": teachers,
                }
            )

        year_summaries[year] = year_items
        (site_dir / "year" / f"{year}.html").write_text(
            render_year_page(year, year_items), encoding="utf-8"
        )
        print(f"  year {year}: {len(year_items)} threads")

    years_sorted = sorted(by_year.keys(), reverse=True)
    (site_dir / "index.html").write_text(
        render_home(years_sorted, epub_years, len(submissions)),
        encoding="utf-8",
    )
    (site_dir / "search-index.json").write_text(
        json.dumps(search_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    # Helpful for GitHub Pages / local servers
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")
    print(f"Finished: {len(submissions)} threads, {len(search_index)} search entries.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate static HTML site from the archive DB.")
    p.add_argument("--db", default=db.DB_DEFAULT, help="SQLite database path")
    p.add_argument(
        "--site-dir",
        default=SITE_DIR_DEFAULT,
        help=f"Output directory (default: {SITE_DIR_DEFAULT}, for GitHub Pages)",
    )
    p.add_argument(
        "--epub-dir",
        default=EPUB_DIR_DEFAULT,
        help=f"Directory of EPUBs to copy into the site (default: {EPUB_DIR_DEFAULT})",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    conn = db.connect(args.db, must_exist=True)
    if not conn:
        return 1
    print(f"Connected to database: {args.db}")
    try:
        generate_site(conn, Path(args.site_dir), Path(args.epub_dir))
    except Exception as e:
        print(f"Error generating site: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
