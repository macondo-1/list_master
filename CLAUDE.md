# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

A CLI automation tool for email marketing operations at SIS International Research. It cleans contact lists from multiple data providers, manages SQLite databases of contacts, and drives email campaigns through GreenArrow (Selenium), SMTP, or OWA (Selenium).

## Running the program

```bash
# Activate the virtual environment first
source .env/bin/activate   # or .myenv/bin/activate

# Run the interactive CLI
python lists.py   # or the root-level entry-point scripts

# Run a specific module directly (for testing/debugging)
python -m modules.database
python -m modules.greenarrow_bot
```

There is no formal test suite. Testing is done interactively in `notes.ipynb`.

## Architecture

### Two parallel code trees

The root level (e.g. `bcc_bot.py`, `greenarrow_bot.py`, `lists.py`) contains older standalone scripts. **`modules/` is the active codebase** and is what new work should use. Root-level files import `constants` directly; `modules/` files import `modules.constants as const`.

### Key modules

| Module | Class/Contents | Responsibility |
|---|---|---|
| `modules/constants.py` | constants | All paths (Google Drive-based), SMTP config, per-source column mappers (RU/Apollo/Hunter/Snov) |
| `modules/lists.py` | `list_`, `NewList` | Reading, normalizing columns, cleaning, deduping, splitting lists |
| `modules/database.py` | `Database`, `Log`, helpers | SQLite connections, filter-based queries, mail merge log |
| `modules/greenarrow_bot.py` | `Hetz_ga(webdriver.Chrome)` | Selenium bot for GreenArrow: list import, campaign setup and send |
| `modules/smtp_bot.py` | `SMTP` | Direct SMTP sending via GoDaddy accounts |
| `modules/bcc_bot.py` / `modules/sm_api.py` | functions | OWA Selenium sending, SurveyMonkey API, shared helpers (`fixing_df_bis`, `update_log`) |
| `modules/utilities.py` | functions | Column mapper registry CRUD (`utilities/mappers.json`) |
| `modules/display.py` | `Display` | CLI menu and title bar |

### Data flow

1. Raw CSV/XLSX files land in `PROCESSING_FOLDER` (Google Drive path defined in `constants.py`)
2. `list_.fix_columns()` detects the source (RU, Apollo, Hunter, Snov) from column names and applies the right mapper, falling back to `utilities/mappers.json` for custom mappers, or interactive creation
3. `list_.FixRecords()` normalizes `first_name` and `email`, drops duplicates/invalids
4. `list_.CleanBlacklisted()` removes emails matching the blacklist file
5. Clean lists are either uploaded to GreenArrow via `Hetz_ga` or sent directly through SMTP/OWA
6. All sends are logged to `LOG_PATH` CSV; `Log` class reads this for dedup and summaries

### File naming convention

List files are prefixed with a project number: `{project_number}_{descriptor}.csv` (e.g. `1002481_ru_test.csv`). The project number links to `blast_master_good_final.xlsx` which holds campaign metadata (template name, GreenArrow server, blast message, schedule).

### `_concurrency` variants

Many interactive functions (that call `input()`) have a `_concurrency` counterpart that accepts parameters directly. Use the `_concurrency` versions when calling programmatically.

### External file dependencies

All runtime data lives on Google Drive (`BASE_PATH` in `constants.py`). The repo itself only holds code and `utilities/` support files. If paths break, check `modules/constants.py` first.

### Sub-projects

`modules/sis_international/` and `modules/email_bison_api/` are separate sub-projects with their own `main.py` and `constants.py`. They are imported selectively from the main codebase.
