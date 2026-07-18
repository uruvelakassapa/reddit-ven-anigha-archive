"""Build yearly EPUB and PDF archives from full Markdown via Pandoc."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

MARKDOWN_DIR_DEFAULT = "markdown_files"
BOOKS_DIR_DEFAULT = "books"

# Tried in order when --mainfont=auto (CI has DejaVu; macOS often has Times/Georgia).
PDF_FONT_CANDIDATES = (
    "DejaVu Serif",
    "Liberation Serif",
    "Times New Roman",
    "Georgia",
    "Palatino",
)
PDF_ENGINE_DEFAULT = "xelatex"

# ven_anigha_reddit_archive_full_2024.md → year 2024
_FULL_MD_RE = re.compile(r"ven_anigha_reddit_archive_full_(\d{4})\.md$")


def find_full_markdown(md_dir: Path) -> list[tuple[int, Path]]:
    found: list[tuple[int, Path]] = []
    for path in sorted(md_dir.glob("ven_anigha_reddit_archive_full_*.md")):
        match = _FULL_MD_RE.search(path.name)
        if match:
            found.append((int(match.group(1)), path))
    return found


def require_cmd(name: str, install_hint: str) -> str:
    path = shutil.which(name)
    if not path:
        print(f"Error: {name} not found on PATH. {install_hint}", file=sys.stderr)
        sys.exit(1)
    return path


def resolve_mainfont(requested: str) -> str:
    """Pick a font XeLaTeX can load. Use --mainfont=auto to probe the system."""
    if requested and requested.lower() != "auto":
        return requested

    fc_list = shutil.which("fc-list")
    for font in PDF_FONT_CANDIDATES:
        if fc_list:
            probe = subprocess.run(
                [fc_list, font],
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0 and probe.stdout.strip():
                print(f"Using PDF mainfont: {font}")
                return font
        # Fallback probe without fontconfig: assume common macOS fonts exist
        if font in ("Times New Roman", "Georgia", "Palatino") and sys.platform == "darwin":
            print(f"Using PDF mainfont: {font}")
            return font

    # Last resort — may still fail on bare systems
    fallback = PDF_FONT_CANDIDATES[0]
    print(f"Warning: could not confirm a PDF font; trying {fallback}", file=sys.stderr)
    return fallback


def build_epub(pandoc: str, md_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        pandoc,
        str(md_path),
        "-o",
        str(out_path),
        "--toc",
        "--toc-depth=2",
        "-f",
        "markdown",
        "-t",
        "epub3",
    ]
    print(f"  EPUB  {md_path.name} → {out_path}")
    subprocess.run(cmd, check=True)


def build_pdf(
    pandoc: str,
    md_path: Path,
    out_path: Path,
    *,
    engine: str,
    mainfont: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        pandoc,
        str(md_path),
        "-o",
        str(out_path),
        "--toc",
        "--toc-depth=2",
        "-f",
        "markdown",
        f"--pdf-engine={engine}",
        "-V",
        f"mainfont={mainfont}",
    ]
    print(f"  PDF   {md_path.name} → {out_path}")
    subprocess.run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate yearly EPUB and PDF files from full Markdown archives."
    )
    parser.add_argument(
        "--md-dir",
        default=MARKDOWN_DIR_DEFAULT,
        help=f"Directory with full Markdown files (default: {MARKDOWN_DIR_DEFAULT}).",
    )
    parser.add_argument(
        "--books-dir",
        default=BOOKS_DIR_DEFAULT,
        help=f"Output directory for EPUB and PDF (default: {BOOKS_DIR_DEFAULT}).",
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        help="Only build this year (repeatable). Default: all years found.",
    )
    parser.add_argument(
        "--format",
        choices=("all", "epub", "pdf"),
        default="all",
        help="Which formats to build (default: all).",
    )
    parser.add_argument(
        "--pdf-engine",
        default=PDF_ENGINE_DEFAULT,
        help=f"Pandoc PDF engine (default: {PDF_ENGINE_DEFAULT}).",
    )
    parser.add_argument(
        "--mainfont",
        default="auto",
        help='PDF main font for xelatex (default: auto — pick first available).',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pandoc = require_cmd("pandoc", "Install it (e.g. brew install pandoc).")
    md_dir = Path(args.md_dir)
    books_dir = Path(args.books_dir)
    want_epub = args.format in ("all", "epub")
    want_pdf = args.format in ("all", "pdf")
    mainfont = resolve_mainfont(args.mainfont) if want_pdf else ""

    if want_pdf:
        require_cmd(
            args.pdf_engine,
            f"Install a TeX distribution that provides {args.pdf_engine} "
            f"(e.g. MacTeX, or apt install texlive-xetex).",
        )

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

    formats = []
    if want_epub:
        formats.append("EPUB")
    if want_pdf:
        formats.append("PDF")
    print(f"Generating {' + '.join(formats)} for {len(sources)} year(s) → {books_dir}/")

    try:
        for year, md_path in sources:
            base = books_dir / f"ven_anigha_reddit_archive_{year}"
            if want_epub:
                build_epub(pandoc, md_path, base.with_suffix(".epub"))
            if want_pdf:
                build_pdf(
                    pandoc,
                    md_path,
                    base.with_suffix(".pdf"),
                    engine=args.pdf_engine,
                    mainfont=mainfont,
                )
    except subprocess.CalledProcessError as e:
        print(f"Error: pandoc failed with exit code {e.returncode}", file=sys.stderr)
        return 1

    print(f"Finished. Books in {books_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
