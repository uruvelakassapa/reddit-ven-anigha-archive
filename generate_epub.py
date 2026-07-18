"""Backward-compatible entry point; prefer generate_books.py. """

from generate_books import main

if __name__ == "__main__":
    raise SystemExit(main())
