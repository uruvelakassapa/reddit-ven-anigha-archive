"""Build yearly EPUB archives from full Markdown via Pandoc."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

MARKDOWN_DIR_DEFAULT = "markdown_files"
EPUB_DIR_DEFAULT = "epub"

# ven_anigha_reddit_archive_full_2024.md → year 2024
_FULL_MD_RE = re.compile(r"ven_anigha_reddit_archive_full_(\d{4})\.md$")


def find_full_markdown(md_dir: Path) -> list[tuple[int, Path]]:
    found: list[tuple[int, Path]] = []
    for path in sorted(md_dir.glob("ven_anigha_reddit_archive_full_*.md")):
        match = _FULL_MD_RE.search(path.name)
        if match:
            found.append((int(match.group(1)), path))
    return found


def require_pandoc() -> str:
    pandoc = shutil.which("pandoc")
    if not pandoc:
        print(
            "Error: pandoc not found on PATH. Install it (e.g. brew install pandoc).",
            file=sys.stderr,
        )
        sys.exit(1)
    return pandoc


def build_epub(pandoc: str, md_path: Path, epub_path: Path) -> None:
    epub_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        pandoc,
        str(md_path),
        "-o",
        str(epub_path),
        "--toc",
        "--toc-depth=2",
        "-f",
        "markdown",
        "-t",
        "epub3",
    ]
    print(f"  {md_path.name} → {epub_path}")
    subprocess.run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate yearly EPUB files from full Markdown archives."
    )
    parser.add_argument(
        "--md-dir",
        default=MARKDOWN_DIR_DEFAULT,
        help=f"Directory with full Markdown files (default: {MARKDOWN_DIR_DEFAULT}).",
    )
    parser.add_argument(
        "--epub-dir",
        default=EPUB_DIR_DEFAULT,
        help=f"Output directory for EPUB files (default: {EPUB_DIR_DEFAULT}).",
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        help="Only build this year (repeatable). Default: all years found.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pandoc = require_pandoc()
    md_dir = Path(args.md_dir)
    epub_dir = Path(args.epub_dir)

    if not md_dir.is_dir():
        print(f"Error: markdown directory not found: {md_dir}", file=sys.stderr)
        return 1

    sources = find_full_markdown(md_dir)
    if args.year:
        wanted = set(args.year)
        sources = [(y, p) for y, p in sources if y in wanted]
        missing = wanted - {y for y, _ in sources}
        if missing:
            print(f"Error: no full Markdown for year(s): {sorted(missing)}", file=sys.stderr)
            return 1

    if not sources:
        print(f"Error: no full Markdown files in {md_dir}", file=sys.stderr)
        return 1

    print(f"Generating {len(sources)} EPUB file(s)…")
    try:
        for year, md_path in sources:
            out = epub_dir / f"ven_anigha_reddit_archive_{year}.epub"
            build_epub(pandoc, md_path, out)
    except subprocess.CalledProcessError as e:
        print(f"Error: pandoc failed with exit code {e.returncode}", file=sys.stderr)
        return 1

    print(f"Finished. EPUBs in {epub_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
