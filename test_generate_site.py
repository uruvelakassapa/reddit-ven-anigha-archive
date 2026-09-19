"""Checks for generate_site offline listing, quotes, and thread parents."""

from pathlib import Path
import tempfile

from generate_archive import format_comment_markdown
from generate_site import (
    _CSS,
    _PARENTS_JS,
    body_to_html,
    collect_offline_files,
    count_parent_nodes,
    page_shell,
    render_comment,
    render_thread,
    render_thread_page,
)
from threads import build_comment_threads


def _row(
    id,
    *,
    author="Bhikkhu_Anigha",
    body="answer",
    parent_id=None,
    parent_author=None,
    parent_body=None,
    parent_permalink=None,
    parent_parent_id=None,
    created=1.0,
):
    return {
        "id": id,
        "parent_id": parent_id,
        "author": author,
        "comment_body": body,
        "permalink": f"https://reddit.com/{id}",
        "created_utc": created,
        "parent_author": parent_author,
        "parent_body": parent_body,
        "parent_permalink": parent_permalink,
        "parent_parent_id": parent_parent_id,
    }


def test_offline_and_shell() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "year").mkdir()
        (root / "thread").mkdir()
        (root / "year" / "2024.html").write_text("x", encoding="utf-8")
        (root / "thread" / "abc.html").write_text("x", encoding="utf-8")
        files = collect_offline_files(root)
        assert "index.html" in files
        assert "assets/offline.js" in files
        assert "assets/parents.js" in files
        assert "sw.js" in files
        assert "year/2024.html" in files
        assert "thread/abc.html" in files
        assert all(not f.startswith("books/") for f in files)

    home = page_shell("Home", "    <p>hi</p>", root="")
    assert 'data-root="./"' in home
    assert 'src="assets/offline.js"' in home
    assert 'src="assets/parents.js"' in home
    assert 'id="offline-save"' in home
    assert "va-show-parents" in home
    assert "html:not(.show-parents) .comment.parent { display: none; }" in _CSS
    assert "scrollIntoView" in _PARENTS_JS

    thread = page_shell("T", "    <p>hi</p>", root="../")
    assert 'data-root="../"' in thread
    assert 'src="../assets/offline.js"' in thread
    assert 'src="../assets/parents.js"' in thread


def test_body_to_html_quotes() -> None:
    html = body_to_html("> excerpt\n\nanswer")
    assert "<blockquote>" in html
    assert "excerpt" in html
    assert "<p>answer</p>" in html
    assert html.find("<blockquote>") < html.find("<p>answer</p>")
    assert "&gt;excerpt" not in html

    nested = body_to_html(">> inner\n> outer")
    assert nested.count("<blockquote>") >= 1
    assert "inner" in nested
    assert "outer" in nested

    plain = body_to_html("See MN 44 for this.")
    assert "<blockquote>" not in plain
    assert "suttacentral.net/mn44" in plain


def test_synthetic_parents_and_no_quote_prefix() -> None:
    comments = [
        _row(
            "a1",
            body="> selected\n\nmy reply",
            parent_id="p1",
            parent_author="layperson",
            parent_body="the whole parent question",
            parent_permalink="https://reddit.com/p1",
            created=10,
        ),
        _row(
            "a2",
            body="second reply",
            parent_id="p1",
            parent_author="layperson",
            parent_body="the whole parent question",
            parent_permalink="https://reddit.com/p1",
            created=20,
        ),
        _row("a3", body="top-level", created=5),
        _row("a4", body="follow-up", parent_id="a3", created=30),
    ]

    without = build_comment_threads(comments)
    assert [n.id for n in without] == ["a3", "a1", "a2"]
    assert without[0].children[0].id == "a4"

    roots = build_comment_threads(comments, include_missing_parents=True)
    ids = [n.id for n in roots]
    assert ids == ["a3", "p1"]
    parent = roots[1]
    assert parent.synthetic
    assert parent.user == "layperson"
    assert [c.id for c in parent.children] == ["a1", "a2"]

    html = render_thread(parent)
    assert 'class="comment parent"' in html
    assert 'class="hd"' in html
    assert "parent-quote" not in html
    assert "the whole parent question" in html
    assert 'class="comment teacher"' in html
    assert html.count("the whole parent question") == 1
    assert "<blockquote>" in html
    assert "selected" in html
    assert "my reply" in html
    assert count_parent_nodes(roots) == 1

    child = render_comment(without[0].children[0])
    assert "parent-quote" not in html
    assert "parent-quote" not in child
    assert "top-level" not in child

    page = render_thread_page(
        {
            "title": "T",
            "author": "op",
            "subreddit": "r/x",
            "created_at": 1,
            "link": "https://reddit.com/t",
            "body": "op body",
        },
        comments,
        year=2025,
    )
    assert 'id="show-parents"' in page
    assert "Show 1 user reply" in page
    assert page.count('class="comment parent"') == 1
    assert "html:not(.show-parents) .comment.parent { display: none; }" in _CSS
    assert 'classList.toggle("show-parents", on)' in _PARENTS_JS

    teachers_only = render_thread_page(
        {
            "title": "T",
            "author": "op",
            "subreddit": "r/x",
            "created_at": 1,
            "link": "https://reddit.com/t",
            "body": "op body",
        },
        [_row("a3", body="top-level")],
        year=2025,
    )
    assert 'id="show-parents"' not in teachers_only
    assert 'class="comment parent"' not in teachers_only


def test_full_markdown_nests_parent_not_blockquote() -> None:
    comments = [
        _row(
            "a1",
            body="> selected\n\nmy reply",
            parent_id="p1",
            parent_author="layperson",
            parent_body="> quoted anigha\n\nquestion",
            parent_permalink="https://reddit.com/p1",
        )
    ]
    roots = build_comment_threads(comments, include_missing_parents=True)
    md = format_comment_markdown(roots[0], include_parents=True, level=0)
    assert "*(In reply to" not in md
    assert md.startswith("- **[layperson]")
    assert "    question" in md
    assert "    - **[Bhikkhu_Anigha]" in md
    assert md.index("question") < md.index("Bhikkhu_Anigha")


def test_synthetic_parent_nests_under_known_grandparent() -> None:
    comments = [
        _row("t1", body="teacher first", created=1),
        _row("t2", body="teacher again", parent_id="u1", parent_author="user",
             parent_body="follow-up question", parent_parent_id="t1", created=3),
        # grandparent not archived -> stays a root
        _row("t3", body="elsewhere", parent_id="u2", parent_author="user",
             parent_body="other question", parent_parent_id="zzz", created=5),
    ]
    roots = build_comment_threads(comments, include_missing_parents=True)
    assert [n.id for n in roots] == ["t1", "u2"]
    assert [c.id for c in roots[0].children] == ["u1"]
    assert [c.id for c in roots[0].children[0].children] == ["t2"]

    # linear chain: no branch labels; a real branch gets one
    assert "replying to" not in render_thread(roots[0])
    comments.append(_row("t4", body="side reply", parent_id="t1", created=4))
    roots = build_comment_threads(comments, include_missing_parents=True)
    page = render_thread(roots[0])
    assert page.count("replying to") == 1
    assert 'href="#c-t1">↳ replying to Bhikkhu_Anigha' in page


def main() -> None:
    test_offline_and_shell()
    test_body_to_html_quotes()
    test_synthetic_parents_and_no_quote_prefix()
    test_full_markdown_nests_parent_not_blockquote()
    test_synthetic_parent_nests_under_known_grandparent()
    print("ok")


if __name__ == "__main__":
    main()
