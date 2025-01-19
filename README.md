This repo archives all comments by [Venerable Anīgha on Reddit](https://www.reddit.com/user/Bhikkhu_Anigha/comments/) then generates readable files.

The sqlite database contains all comments by Ven. Anīgha as well as the original questions. This is database is populated by `fetch_comments.py`.

Then markdown, pdf, and epub files are created by year with `generate_markdown.py`. The resulting files are in the `*_files/` folders.

Both database and output files are run every midnight Sunday UTC.

Most of the time, comments from Reddit have quotes markers in the correct place. However, there are some quotes that don't have their markers. Some comments have paragraphs that seem like Anigha is saying it, but it's actually a quote of the other person. This issue is quite rare though and it's very clear when Anigha is speaking in the text.

## Todo

- [x] automatic pdf generation
- [x] automatic epub generation
- [ ] Add date of generation to the files.
- [ ] (maybe) include the parent comments within threads as opposed to only the original question.
- [ ] Fix questions that were deleted and show up as [removed] in the database.
- [ ] Fix issue where emojis are missing from PDFs.
- [ ] Make external links in the PDFs more noticable. Links work but are hard to distinguish from normal text because they don't have a different style.

## Packages for PDF local package generation

[Install pandocs and PDF generation for the proper OS by following this link.](https://pandoc.org/installing.html)

[Install the latest Source Serif font from this repo.](https://github.com/adobe-fonts/source-serif/releases) This should be the *_Desktop.zip, not *_WOFF.zip which is intended for web.

For Windows, unpack the zip file, select all the *.ttf fonts, right click, then click on "install" to add the fonts to the OS.

Installing fonts in other OS's should be very similar.

## Setup and run locally

1. Install Python 3.13 or some other reasonable modern version of Python.
2. Create venv `python -m .venv` and activate it for your OS, such as, `source .venv/Scripts/activate` for Windows.
3. `pip install -e .[test,style]` to install all the test and style dependencies of the project.
4. Setup `.secrets` file with proper values for Reddit and script adjustment.
5. Run `python src/fetch_comments.py` to generate `reddit_comments.db` with the latest comments.
6. Run `python src/generate_markdown.py` to generate the markdown, epub, and pdf files.
7. Run `pytest --cov` to test all the code with coverage.

## Running github workflow locally.

Install [act](https://github.com/nektos/act) to run the github workflow locally. This is very useful for debugging the workflow.

Run `act -j update_comments --secret-file .secrets --artifact-server-path $PWD/.artifacts`.

`.secrets` is an env file that should contain all the secrets used to run the github workflow.

`.artfacts` is a folder that stores the db between runs.

This stalls at the final push for some reason.