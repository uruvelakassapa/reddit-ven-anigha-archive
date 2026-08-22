"""Generate a static HTML site from the SQLite archive (web viewer)."""

from __future__ import annotations

import argparse
import datetime
import html
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Sequence

import markdown as md_lib

import db
from threads import ThreadNode, build_comment_threads

SITE_DIR_DEFAULT = "docs"
BOOKS_DIR_DEFAULT = "books"
REPO_URL = "https://github.com/uruvelakassapa/reddit-ven-anigha-archive"
SITE_URL = "https://uruvelakassapa.github.io/reddit-ven-anigha-archive/"
HH_URL = "https://www.hillsidehermitage.org/"
TEACHERS = frozenset({"Bhikkhu_Anigha", "Sister_Medhini"})

# Conservative sutta refs only (web viewer enrichment — not applied to MD/EPUB).
_SUTTA_RE = re.compile(r"\b(DN|MN|SN|AN)\s*(\d+(?:\.\d+)?)\b", re.IGNORECASE)
# Split HTML into tags vs text so sutta linking never rewrites hrefs.
_HTML_TOKEN_RE = re.compile(r"(<[^>]+>)")

_MD_EXTENSIONS = ["nl2br", "sane_lists", "fenced_code", "tables"]

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
header.site .header-row {
  display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 0.5rem 1rem;
}
header.site nav.top-links {
  display: flex; flex-wrap: wrap; gap: 0.35rem 1rem; font-size: 0.9rem; opacity: 0.95;
}
header.site nav.top-links a { text-decoration: none; border-bottom: 1px solid transparent; }
header.site nav.top-links a:hover { border-bottom-color: rgba(232, 240, 232, 0.7); }
nav.crumbs { font-size: 0.9rem; margin: 1rem 0; color: var(--muted); }
main { padding: 0 1.25rem 3rem; }
footer.site {
  border-top: 1px solid var(--border);
  padding: 1.25rem;
  color: var(--muted);
  font-size: 0.9rem;
}
footer.site a { color: var(--link); }
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
.offline-bar {
  margin: 0.7rem 0 0;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
}
header.site .offline-bar .chip { color: var(--fg); }
#offline-status { font-size: 0.88rem; opacity: 0.9; }
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
.comment .body, .op { margin-top: 0.6rem; }
.md-body p { margin: 0 0 0.75rem; }
.md-body p:last-child { margin-bottom: 0; }
.md-body a { word-break: break-word; }
.md-body blockquote {
  margin: 0.5rem 0 0.75rem;
  padding: 0.35rem 0 0.35rem 0.85rem;
  border-left: 3px solid #cbbfae;
  color: var(--muted);
}
.md-body ul, .md-body ol { margin: 0.4rem 0 0.75rem; padding-left: 1.4rem; }
.md-body li { margin: 0.2rem 0; }
.md-body code {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.9em;
  background: rgba(0,0,0,0.05);
  padding: 0.1em 0.3em;
  border-radius: 3px;
}
.md-body pre {
  overflow-x: auto;
  padding: 0.75rem 1rem;
  background: rgba(0,0,0,0.05);
  border-radius: 6px;
}
.md-body pre code { background: none; padding: 0; }
.md-body strong { font-weight: 700; }
.md-body em { font-style: italic; }
.md-body hr { border: none; border-top: 1px solid var(--border); margin: 1rem 0; }
.children { margin-left: 1rem; padding-left: 0.5rem; border-left: 2px solid var(--border); }
.comments-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.75rem;
  flex-wrap: wrap;
  margin: 1.25rem 0 0.5rem;
}
.comments-head h3 { margin: 0; }
.parent-toggle {
  font-size: 0.9rem;
  color: var(--muted);
  cursor: pointer;
  user-select: none;
}
.parent-toggle input { margin-right: 0.35rem; }
.comment.parent {
  border: none;
  background: transparent;
  padding: 0;
  margin: 0;
}
.comment.parent > :not(.children) { display: none; }
.comment.parent > .children {
  margin-left: 0;
  padding-left: 0;
  border-left: none;
}
html.show-parents .comment.parent {
  margin: 1rem 0;
  padding: 0.85rem 1rem;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--card);
}
html.show-parents .comment.parent > :not(.children) { display: block; }
html.show-parents .comment.parent > .children {
  margin-left: 1rem;
  padding-left: 0.5rem;
  border-left: 2px solid var(--border);
}
.books-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.95rem;
}
.books-table th,
.books-table td {
  text-align: left;
  padding: 0.55rem 0.4rem;
  border-bottom: 1px solid var(--border);
  vertical-align: middle;
}
.books-table th { color: var(--muted); font-weight: 600; }
.books-table td.yr { font-weight: 600; width: 4rem; }
.books-table a {
  display: inline-block;
  padding: 0.3rem 0.65rem;
  margin-right: 0.25rem;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 6px;
  text-decoration: none;
  white-space: nowrap;
}
.books-table a:hover { border-color: var(--accent); }
.books-table .size { color: var(--muted); font-size: 0.85rem; margin-left: 0.2rem; }
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

CACHE_NAME = "va-archive-v3"

_PARENTS_JS = """\
(function () {
  const KEY = "va-show-parents";
  const box = document.getElementById("show-parents");
  if (!box) return;
  if (!document.querySelector(".comment.parent")) {
    const wrap = box.closest(".parent-toggle");
    if (wrap) wrap.hidden = true;
    return;
  }
  function apply(on) {
    document.documentElement.classList.toggle("show-parents", on);
    box.checked = on;
  }
  let stored = false;
  try { stored = localStorage.getItem(KEY) === "1"; } catch (e) {}
  apply(stored);
  box.addEventListener("change", function () {
    const on = box.checked;
    try { localStorage.setItem(KEY, on ? "1" : "0"); } catch (e) {}
    apply(on);
  });
})();
"""

_SW_JS = f"""\
const CACHE = "{CACHE_NAME}";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => {{
  event.waitUntil((async () => {{
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)));
    await self.clients.claim();
  }})());
}});

async function fromCache(request) {{
  const cache = await caches.open(CACHE);
  const match = await cache.match(request);
  if (match) return match;
  const ignored = await cache.match(request, {{ ignoreSearch: true }});
  if (ignored) return ignored;
  const url = new URL(request.url);
  if (url.pathname.endsWith("/")) {{
    return cache.match(new URL("index.html", url).href);
  }}
  return undefined;
}}

self.addEventListener("fetch", (event) => {{
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  event.respondWith((async () => {{
    const cached = await fromCache(req);
    const network = fetch(req).then((resp) => {{
      if (resp && resp.ok) {{
        caches.open(CACHE).then((cache) => cache.put(req, resp.clone()));
      }}
      return resp;
    }}).catch(() => undefined);
    if (cached) {{
      void network;
      return cached;
    }}
    const resp = await network;
    if (resp) return resp;
    return new Response("Offline", {{
      status: 503,
      headers: {{ "Content-Type": "text/plain" }},
    }});
  }})());
}});
"""

_OFFLINE_JS = f"""\
(function () {{
  const CACHE = "{CACHE_NAME}";
  const script = document.currentScript;
  const rootAttr = (script && script.getAttribute("data-root")) || "./";
  const siteRoot = new URL(rootAttr, location.href);
  const bar = document.querySelector(".offline-bar");
  const btn = document.getElementById("offline-save");
  const status = document.getElementById("offline-status");
  if (!bar || !btn || !status) return;
  if (!("serviceWorker" in navigator) || !("caches" in window)) return;

  bar.hidden = false;

  function setStatus(text) {{
    status.textContent = text || "";
  }}

  function markSaved() {{
    btn.hidden = true;
    setStatus("Available offline");
  }}

  navigator.serviceWorker
    .register(new URL("sw.js", siteRoot), {{ updateViaCache: "none" }})
    .catch(function () {{}});

  async function fileList() {{
    const resp = await fetch(new URL("offline-files.json", siteRoot));
    if (!resp.ok) throw new Error("Could not load file list");
    return resp.json();
  }}

  async function alreadySaved(files) {{
    try {{
      const n = localStorage.getItem("va-offline-n");
      if (!(n && Number(n) >= files.length)) return false;
      const cache = await caches.open(CACHE);
      const keys = await cache.keys();
      return keys.length >= files.length;
    }} catch (e) {{
      return false;
    }}
  }}

  fileList()
    .then(async function (files) {{
      if (await alreadySaved(files)) markSaved();
    }})
    .catch(function () {{}});

  btn.addEventListener("click", async function () {{
    btn.disabled = true;
    setStatus("Saving\\u2026");
    try {{
      const files = await fileList();
      const cache = await caches.open(CACHE);
      let done = 0;
      let failed = 0;
      const total = files.length;
      const chunk = 8;
      for (let i = 0; i < files.length; i += chunk) {{
        const batch = files.slice(i, i + chunk);
        await Promise.all(batch.map(async function (file) {{
          const url = new URL(file, siteRoot).href;
          try {{
            const hit = await cache.match(url);
            if (!hit) {{
              const resp = await fetch(url, {{ credentials: "same-origin" }});
              if (!resp.ok) throw new Error(String(resp.status));
              await cache.put(url, resp);
            }}
          }} catch (e) {{
            failed += 1;
          }}
          done += 1;
          setStatus("Saving " + done + "/" + total + "\\u2026");
        }}));
      }}
      try {{
        localStorage.setItem("va-offline-n", String(total - failed));
      }} catch (e) {{}}
      if (failed) {{
        btn.disabled = false;
        btn.hidden = false;
        setStatus("Saved " + (total - failed) + "/" + total + " \\u2014 retry to finish");
      }} else {{
        markSaved();
      }}
    }} catch (e) {{
      btn.disabled = false;
      setStatus("Could not save offline.");
    }}
  }});
}})();
"""


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


def _sutta_repl(match: re.Match) -> str:
    collection = match.group(1).upper()
    number = match.group(2)
    slug = f"{collection.lower()}{number}"
    label = f"{collection} {number}"
    href = f"https://suttacentral.net/{slug}"
    return (
        f'<a href="{href}" rel="noopener noreferrer" target="_blank">{label}</a>'
    )


def linkify_suttas_in_html(html_text: str) -> str:
    """
    Turn bare DN/MN/SN/AN citations into SuttaCentral links.

    Only rewrites text nodes (not tags/attributes), so existing markdown
    links and URLs are left intact.
    """
    parts = _HTML_TOKEN_RE.split(html_text)
    out = []
    for part in parts:
        if part.startswith("<"):
            out.append(part)
        else:
            out.append(_SUTTA_RE.sub(_sutta_repl, part))
    return "".join(out)


def body_to_html(text: str) -> str:
    """Render Reddit-style Markdown to HTML, then add sutta links."""
    text = (text or "").replace("\r\n", "\n")
    rendered = md_lib.markdown(text, extensions=_MD_EXTENSIONS)
    # Open external links in a new tab where markdown produced plain <a href>
    rendered = re.sub(
        r'<a href="(https?://[^"]+)"',
        r'<a href="\1" rel="noopener noreferrer" target="_blank"',
        rendered,
    )
    return f'<div class="md-body">{linkify_suttas_in_html(rendered)}</div>'


def page_shell(
    title: str,
    body: str,
    *,
    root: str = "",
    description: str = "Archive of Reddit discussions with Ven. Anīgha and Sister Medhini.",
) -> str:
    site_root = root if root else "./"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <meta name="description" content="{html.escape(description)}">
  <link rel="stylesheet" href="{root}assets/style.css">
  <script>
    try {{ if (localStorage.getItem("va-show-parents") === "1") document.documentElement.classList.add("show-parents"); }} catch (e) {{}}
  </script>
</head>
<body>
  <header class="site">
    <div class="inner">
      <div class="header-row">
        <h1><a href="{root}index.html" style="color:inherit;text-decoration:none">Ven Anīgha Reddit Archive</a></h1>
        <nav class="top-links" aria-label="Related sites">
          <a href="{HH_URL}" rel="noopener noreferrer" target="_blank">Hillside Hermitage</a>
          <a href="{REPO_URL}" rel="noopener noreferrer" target="_blank">GitHub</a>
        </nav>
      </div>
      <p>Comments by Bhikkhu Anīgha &amp; Sister Medhini</p>
      <p class="offline-bar" hidden>
        <button type="button" class="chip" id="offline-save">Save offline</button>
        <span id="offline-status" role="status" aria-live="polite"></span>
      </p>
    </div>
  </header>
  <main>
{body}
  </main>
  <footer class="site">
    <div class="inner">
      <a href="{HH_URL}" rel="noopener noreferrer" target="_blank">Hillside Hermitage</a>
      · <a href="{REPO_URL}" rel="noopener noreferrer" target="_blank">Source on GitHub</a>
    </div>
  </footer>
  <script src="{root}assets/offline.js" data-root="{html.escape(site_root, quote=True)}"></script>
  <script src="{root}assets/parents.js"></script>
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
    if node.user in TEACHERS:
        classes = "comment teacher"
    else:
        classes = "comment parent"
    who = html.escape(node.user)
    url = html.escape(node.url, quote=True) if node.url and node.url != "#" else ""

    if url:
        who_html = f'<a class="who" href="{url}">{who}</a>'
    else:
        who_html = f'<span class="who">{who}</span>'
    when_html = ""
    if not node.synthetic:
        when_html = f'<span class="when">{html.escape(format_timestamp(node.created_at))}</span>'

    body = body_to_html(node.content)
    if node.synthetic and not (node.content or "").strip():
        body = '<div class="md-body"><p><em>Comment not available</em></p></div>'

    children_html = ""
    if node.children:
        kids = "\n".join(render_comment(c) for c in node.children)
        children_html = f'<div class="children">\n{kids}\n</div>'

    return f"""<article class="{classes}" id="c-{html.escape(node.id)}">
  <div>{who_html}{when_html}</div>
  <div class="body">{body}</div>
  {children_html}
</article>"""


def render_thread_page(sub, comments, *, year: int) -> str:
    roots = build_comment_threads(comments, include_missing_parents=True)
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
      <div class="comments-head">
        <h3>Comments</h3>
        <label class="parent-toggle">
          <input type="checkbox" id="show-parents">
          Show full user replies
        </label>
      </div>
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


def _format_size(num_bytes: int) -> str:
    if num_bytes >= 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.1f} MB"
    if num_bytes >= 1024:
        return f"{num_bytes / 1024:.0f} KB"
    return f"{num_bytes} B"


def render_home(
    years: Sequence[int],
    thread_count: int,
    *,
    book_catalog: Dict[int, Dict[str, int]],
) -> str:
    """book_catalog: year -> {'epub': bytes, 'pdf': bytes} (keys only if file exists)."""
    year_links = "\n".join(
        f'      <li><a class="title" href="year/{y}.html">{y}</a></li>' for y in years
    )
    if book_catalog:
        rows = []
        for y in sorted(book_catalog.keys(), reverse=True):
            files = book_catalog[y]
            if "epub" in files:
                epub_cell = (
                    f'<a href="books/ven_anigha_reddit_archive_{y}.epub">EPUB</a>'
                    f'<span class="size">{_format_size(files["epub"])}</span>'
                )
            else:
                epub_cell = '<span class="meta">—</span>'
            if "pdf" in files:
                pdf_cell = (
                    f'<a href="books/ven_anigha_reddit_archive_{y}.pdf">PDF</a>'
                    f'<span class="size">{_format_size(files["pdf"])}</span>'
                )
            else:
                pdf_cell = '<span class="meta">—</span>'
            rows.append(
                f"        <tr><td class=\"yr\">{y}</td>"
                f"<td>{epub_cell}</td><td>{pdf_cell}</td></tr>"
            )
        books_block = f"""      <p class="meta">Yearly archives (full parent context). EPUB for e-readers; PDF for print or desktop.</p>
      <table class="books-table">
        <thead><tr><th>Year</th><th>EPUB</th><th>PDF</th></tr></thead>
        <tbody>
{chr(10).join(rows)}
        </tbody>
      </table>"""
    else:
        books_block = '      <p class="meta">No books found. Run <code>generate_books.py</code> first.</p>'

    body = f"""    <section class="card">
      <h2>About</h2>
      <p>This site presents archived Reddit Q&amp;A involving
      <strong>Bhikkhu Anīgha</strong> and <strong>Sister Medhini</strong>
      (chiefly r/HillsideHermitage). See also
      <a href="{HH_URL}" rel="noopener noreferrer" target="_blank">Hillside Hermitage</a>.
      Thread pages show teacher replies; turn on
      <strong>Show full user replies</strong> for the comments they answered.
      Citations like MN 44 are linked to SuttaCentral on this site only.
      Use <strong>Save offline</strong> in the header to keep browse and search
      on this device.</p>
      <p class="meta">{thread_count} threads across {len(years)} year(s).
      Source: <a href="{REPO_URL}" rel="noopener noreferrer" target="_blank">github.com/uruvelakassapa/reddit-ven-anigha-archive</a>.</p>
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
      <h2>Download EPUB &amp; PDF</h2>
{books_block}
    </section>
    <script src="assets/search.js"></script>"""
    return page_shell("Ven Anīgha Reddit Archive", body, root="")


def collect_offline_files(site_dir: Path) -> List[str]:
    """Relative paths the offline cache should store (not EPUB/PDF books)."""
    files = [
        "index.html",
        "assets/style.css",
        "assets/search.js",
        "assets/offline.js",
        "assets/parents.js",
        "search-index.json",
        "sw.js",
        "offline-files.json",
    ]
    year_dir = site_dir / "year"
    thread_dir = site_dir / "thread"
    if year_dir.is_dir():
        files.extend(f"year/{p.name}" for p in sorted(year_dir.glob("*.html")))
    if thread_dir.is_dir():
        files.extend(f"thread/{p.name}" for p in sorted(thread_dir.glob("*.html")))
    return files


def write_assets(site_dir: Path) -> None:
    assets = site_dir / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    (assets / "style.css").write_text(_CSS, encoding="utf-8")
    (assets / "search.js").write_text(_SEARCH_JS, encoding="utf-8")
    (assets / "offline.js").write_text(_OFFLINE_JS, encoding="utf-8")
    (assets / "parents.js").write_text(_PARENTS_JS, encoding="utf-8")
    (site_dir / "sw.js").write_text(_SW_JS, encoding="utf-8")


def copy_books(books_dir: Path, site_dir: Path) -> Dict[int, Dict[str, int]]:
    """Copy EPUB/PDF into site; return year -> {format: size_bytes}."""
    dest = site_dir / "books"
    dest.mkdir(parents=True, exist_ok=True)
    catalog: Dict[int, Dict[str, int]] = {}
    if not books_dir.is_dir():
        return catalog
    for path in sorted(books_dir.glob("ven_anigha_reddit_archive_*.*")):
        match = re.search(r"_(\d{4})\.(epub|pdf)$", path.name, re.I)
        if not match:
            continue
        year = int(match.group(1))
        kind = match.group(2).lower()
        shutil.copy2(path, dest / path.name)
        catalog.setdefault(year, {})[kind] = path.stat().st_size
    return catalog


def generate_site(conn, site_dir: Path, books_dir: Path) -> None:
    if site_dir.exists():
        # Remove generated content but keep site_dir if it's docs with other stuff —
        # for a dedicated output tree, clear known subdirs/files we own.
        for name in (
            "assets",
            "year",
            "thread",
            "books",
            "epub",  # legacy path
            "index.html",
            "search-index.json",
            "sw.js",
            "offline-files.json",
        ):
            path = site_dir / name
            if path.is_dir():
                shutil.rmtree(path)
            elif path.is_file():
                path.unlink()
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "year").mkdir()
    (site_dir / "thread").mkdir()
    write_assets(site_dir)
    book_catalog = copy_books(books_dir, site_dir)

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
        render_home(
            years_sorted,
            len(submissions),
            book_catalog=book_catalog,
        ),
        encoding="utf-8",
    )
    (site_dir / "search-index.json").write_text(
        json.dumps(search_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    offline_files = collect_offline_files(site_dir)
    (site_dir / "offline-files.json").write_text(
        json.dumps(offline_files, separators=(",", ":")),
        encoding="utf-8",
    )
    # Helpful for GitHub Pages / local servers
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")
    print(
        f"Finished: {len(submissions)} threads, {len(search_index)} search entries, "
        f"{len(offline_files)} offline files."
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate static HTML site from the archive DB.")
    p.add_argument("--db", default=db.DB_DEFAULT, help="SQLite database path")
    p.add_argument(
        "--site-dir",
        default=SITE_DIR_DEFAULT,
        help=f"Output directory (default: {SITE_DIR_DEFAULT}, for GitHub Pages)",
    )
    p.add_argument(
        "--books-dir",
        default=BOOKS_DIR_DEFAULT,
        help=f"Directory of EPUB/PDF files to copy into the site (default: {BOOKS_DIR_DEFAULT})",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    conn = db.connect(args.db, must_exist=True)
    if not conn:
        return 1
    print(f"Connected to database: {args.db}")
    try:
        generate_site(conn, Path(args.site_dir), Path(args.books_dir))
    except Exception as e:
        print(f"Error generating site: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
