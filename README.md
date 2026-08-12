# list_master

A CLI automation tool for email marketing operations at SIS International Research.
It cleans contact lists from multiple data providers, manages SQLite databases of
contacts, and drives email campaigns through GreenArrow (Selenium), SMTP, or OWA
(Selenium).

## What it does

- **Cleans lists** — detects the source of a raw CSV/XLSX export (RU, Apollo, Hunter,
  Snov, or a custom mapper) and normalizes it: column names, `first_name`, `email`,
  duplicates, invalid addresses, and blacklist matches.
- **Manages contact data** — stores and queries contacts in SQLite, and logs every
  send to a CSV mail-merge log for dedup and reporting.
- **Drives campaigns** — imports lists into GreenArrow and kicks off blasts, sends
  directly via SMTP (GoDaddy accounts), or sends via Outlook Web Access, all through
  Selenium bots where a UI has to be driven.
- **Integrates with Email Bison and SurveyMonkey** — via their respective APIs.
- **Reports on projects** — pulls together mailing/recruiting numbers across
  projects for a quick health check.

Everything is exposed through one interactive, menu-driven CLI.

## Requirements

- Python 3.14 (pinned in `.python-version`)
- Google Chrome (for the Selenium-driven bots — GreenArrow and OWA)
- Access to the shared Google Drive folder the tool reads/writes list and project
  data from (path configured in `modules/constants.py`, see below)

## Setup

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`modules/constants.py` is **not checked into git** (see `.gitignore`) because it
hardcodes machine-specific, Google Drive-based paths (processing folder, blacklist,
log DB, `blast_master_good_final.xlsx`, credentials file, etc.). Copy it from
another working machine, or recreate it, before running anything — most modules
import it as `import modules.constants as const` and will fail without it.

## Running

```bash
source .venv/bin/activate
./sis_services
```

`sis_services` is the active entry point — a Python script (shebang points at
`.venv/bin/python3`) that wires together everything under `modules/` and drives the
interactive menu:

```
📋 LISTS
[1]  Clean list
[2]  Concatenate lists
[3]  Deduper
[4]  Divide list

📊 DATABASE
[5]  Extract project's filter from internal database

✉️  MAILMERGE
[6]  Create MM list
[7]  Decompose MM list
[8]  BCC MM list
[9]  SMTP MM list
[10] Mailmerge summary

🏹 GREEN ARROW
[11] Import list to GreenArrow
[12] Send GreenArrow blast

🦬 Email Bison
[13] Clean against email bison
[14] Add list to email bison
[15] Create new project in bison
[16] Restart campaigns in bison
[17] Get campaign stats

💻 SYSTEM
[18] Block email(s)
[19] Create project folder
[20] Graph all projects
[Q]  Quit
```

Day-to-day exploratory testing still happens interactively in `notes.ipynb`.
Automated regression tests live under `tests/` — see **Testing** below.

## Testing

```bash
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

The suite covers the list-cleaning and column-mapper logic in `modules/lists.py`
and `modules/utilities.py` — the deterministic parts of the codebase. It does
not cover the SQLite database layer, the Selenium-driven bots (GreenArrow/OWA),
SMTP sending, or the Email Bison/SurveyMonkey integrations. A root-level
`conftest.py` substitutes a fake `modules.constants` module (see
`tests/fixtures/fake_constants.py`) so the suite runs without the real,
gitignored `modules/constants.py`.

## Architecture

### Two parallel code trees

The root level (`bcc_bot.py`, `greenarrow_bot.py`, `lists.py`, `smtp_bot.py`, `sm_api.py`,
`database.py`, `display.py`, ...) contains older standalone scripts kept for
reference. **`modules/` is the active codebase** and is what `sis_services` runs and
what new work should target. Root-level files import `constants` directly; `modules/`
files import `modules.constants as const`.

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

1. Raw CSV/XLSX files land in `PROCESSING_FOLDER` (Google Drive path defined in
   `constants.py`).
2. `list_.fix_columns()` detects the source (RU, Apollo, Hunter, Snov) from column
   names and applies the right mapper, falling back to `utilities/mappers.json` for
   custom mappers, or interactive creation.
3. `list_.FixRecords()` normalizes `first_name` and `email`, drops duplicates/invalids.
4. `list_.CleanBlacklisted()` removes emails matching the blacklist file.
5. Clean lists are either uploaded to GreenArrow via `Hetz_ga` or sent directly
   through SMTP/OWA.
6. All sends are logged to `LOG_PATH` (CSV); the `Log` class reads this for dedup
   and summaries.

### File naming convention

List files are prefixed with a project number: `{project_number}_{descriptor}.csv`
(e.g. `1002481_ru_test.csv`). The project number links to
`blast_master_good_final.xlsx`, which holds campaign metadata (template name,
GreenArrow server, blast message, schedule).

### `_concurrency` variants

Many interactive functions (that call `input()`) have a `_concurrency` counterpart
that accepts parameters directly. Use the `_concurrency` versions when calling
programmatically instead of through the CLI.

### Sub-projects

- `modules/email_bison_api/` — a separate sub-project (its own git repo, tracked as
  a gitlink) with its own `main.py` and `constants.py`, imported selectively as
  `import modules.email_bison_api.main as bison`.
- `modules/sis_international/` — was originally a similar separate sub-project;
  that repo has since been archived and evolved into an unrelated, larger system
  elsewhere. It's now folded directly into this repo as regular tracked files:
  `main.py` (`Project`, `get_working_jsons`,
  `get_all_projects_mailing_and_recruits_numbers` — powers menu option 20, "Graph
  all projects"), plus `README.md` / `workflow.drawio` /
  `files/utilities/base_project_filter.csv` for reference.
  `modules/sis_international/files/database/` and `files/projects/` are real local
  project data, gitignored and not tracked.

### External file dependencies

All runtime data lives on Google Drive (`BASE_PATH` in `constants.py`). The repo
itself only holds code and `utilities/` support files. If paths break, check
`modules/constants.py` first.

## Debugging a single module

```bash
source .venv/bin/activate
python -m modules.database
python -m modules.greenarrow_bot
```
