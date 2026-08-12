# Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pytest suite covering the list-cleaning and column-mapper logic in `modules/lists.py` and `modules/utilities.py`, runnable on a fresh checkout without the real, gitignored `modules/constants.py`.

**Architecture:** A root-level `conftest.py` installs a tracked fake `modules.constants` module into `sys.modules` before any test imports code under `modules/`. Tests live in `tests/`, exercising the existing production functions directly with in-memory DataFrames or small tracked fixture files (JSON mapper registry, blacklist, sample CSVs) under `tests/fixtures/`.

**Tech Stack:** Python 3.14, pytest 9.1.1, pandas 2.2.3 (existing pin).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-11-test-suite-design.md` — this plan implements it in full.
- No production code under `modules/` is modified by this plan. Every expected value below was verified by hand-running the actual production code (`FixRecords`, `fix_columns`, `CleanBlacklisted`, `find_matching_columns`) against pandas 2.2.3 before this plan was written — they are not assumptions.
- This plan tests *existing, already-behaving* code, not new features. Most tasks therefore don't have a true red→green implementation cycle (there's no new production code to write) — where a task's "red" step would just be "the test can't run yet because its fixture doesn't exist," the "green" step is adding that fixture, not touching `modules/`.
- If any test in this plan fails against real, current behavior once it actually runs: that means this plan's documented expected value is wrong, not that production code needs to change. Stop and reconcile the plan/test with reality — do not alter `modules/` code to make a test pass.
- Out of scope (per spec's Non-goals): `modules/database.py`, `modules/greenarrow_bot.py`, `modules/bcc_bot.py`, `modules/smtp_bot.py`, `modules/sm_api.py`, `modules/email_bison_api/`, `modules/display.py`, root-level legacy scripts, and the interactive/`input()`-driven paths (`list_.clean_list_manually`, `create_new_column_mapper`) and deprecated functions (`list_.FixColumns`, `list_.FixUnknownColumns`).
- Existing call convention in this codebase: `list_` methods are called unbound on the class itself, e.g. `list_.FixRecords(df)`, never on an instance. Match this in all new test code.
- Fixture content must be synthetic — never copy real entries from the gitignored `modules/constants.py` or `utilities/mappers.json`.

---

### Task 1: Test infrastructure bootstrap

**Files:**
- Create: `requirements-dev.txt`
- Create: `pytest.ini`
- Create: `conftest.py` (repo root)
- Create: `tests/fixtures/fake_constants.py`
- Create: `tests/test_smoke.py`

**Interfaces:**
- Produces: `sys.modules['modules.constants']` populated with `tests/fixtures/fake_constants` before test collection — every later task relies on this to import `modules.lists` / `modules.utilities` at all.
- Produces: `fake_constants.FILE_COLUMNS_DICT_PATH` (`Path`, defaults to `tests/fixtures/sample_mappers.json`), `fake_constants.BLACKLIST_PATH` (`Path`, defaults to `tests/fixtures/sample_blacklist.txt`), `fake_constants.DB_COLUMNS` (`list[str]`) — attributes later tasks read directly or override via `monkeypatch.setattr(fake_constants, "ATTR", value)`.

- [ ] **Step 1: Add the dev dependency and install it**

Create `requirements-dev.txt`:

```
-r requirements.txt
pytest==9.1.1
```

Run: `pip install -r requirements-dev.txt`
Expected: pytest installs cleanly (verified compatible with Python 3.14 and this repo's pinned pandas 2.2.3).

- [ ] **Step 2: Write the failing smoke test**

Create `tests/test_smoke.py`:

```python
def test_modules_lists_and_utilities_import_cleanly():
    import modules.lists  # noqa: F401
    import modules.utilities  # noqa: F401
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `pytest tests/test_smoke.py -v`
Expected: FAIL/ERROR with `ModuleNotFoundError` (either `No module named 'modules'`, because nothing has put the repo root on `sys.path` yet, or `No module named 'modules.constants'`, because that file is gitignored and doesn't exist on this checkout).

- [ ] **Step 4: Add the constants shim and pytest config**

Create `tests/fixtures/fake_constants.py`:

```python
"""
Fake stand-in for the real, gitignored `modules/constants.py`.

Installed into `sys.modules['modules.constants']` by the repo-root
`conftest.py` before any test imports `modules.lists` or
`modules.utilities`. Values are dummy/synthetic — nothing here is copied
from the real constants file. Individual tests override specific
attributes with `monkeypatch.setattr(fake_constants, "ATTR", value)` to
point at per-test fixture data instead of the defaults below.
"""
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent

# modules.lists.fix_columns / modules.utilities.add_new_column_mapper
FILE_COLUMNS_DICT_PATH = FIXTURES_DIR / 'sample_mappers.json'

# modules.lists.CleanBlacklisted
BLACKLIST_PATH = FIXTURES_DIR / 'sample_blacklist.txt'

# modules.utilities.create_new_column_mapper (not under test)
DB_COLUMNS = ['first_name', 'email']
```

Create `conftest.py` at the repo root (same directory as `README.md` and `sis_services`):

```python
"""
Root conftest.py.

Two jobs, both load-order-sensitive:

1. `modules/constants.py` is gitignored and machine-specific (real Google
   Drive paths, credentials). It doesn't exist on a fresh checkout or in
   CI, so `import modules.lists` / `import modules.utilities` would fail
   before any test runs. We swap in a tracked fake module before those
   modules get imported anywhere in the test session.
2. There's no `__init__.py` anywhere in this repo and no installed
   package. Having *any* conftest.py at the repo root makes pytest add
   the repo root (rather than tests/) to sys.path via its "prepend"
   import-mode basedir logic — that's what makes `import modules.lists`
   resolve at all from inside tests/test_*.py.
"""
import sys

from tests.fixtures import fake_constants

sys.modules['modules.constants'] = fake_constants
```

Create `pytest.ini` at the repo root:

```ini
[pytest]
testpaths = tests
```

- [ ] **Step 5: Run the smoke test again and confirm it passes**

Run: `pytest tests/test_smoke.py -v`
Expected: `1 passed`

- [ ] **Step 6: Commit**

```bash
git add requirements-dev.txt pytest.ini conftest.py tests/
git commit -m "Add pytest infrastructure and a constants import shim"
```

---

### Task 2: `FixRecords` tests

**Files:**
- Create: `tests/test_lists.py`

**Interfaces:**
- Consumes: `modules.lists.list_.FixRecords(df: pd.DataFrame) -> pd.DataFrame`, called unbound as `list_.FixRecords(df)`.
- Produces: `_records(first_names, emails) -> pd.DataFrame` helper, reused by Task 4's test in this same file (Task 3 and Task 6 need raw, unrenamed column names, so they build DataFrames directly instead).

- [ ] **Step 1: Write the tests**

Create `tests/test_lists.py`:

```python
import pandas as pd

from modules.lists import list_


def _records(first_names, emails):
    return pd.DataFrame({'first_name': first_names, 'email': emails})


def test_fixrecords_none_or_nan_first_name_becomes_colleague():
    df = _records([None, float('nan')], ['a@x.com', 'b@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Colleague', 'Colleague']


def test_fixrecords_keeps_first_token_of_multiword_name():
    df = _records(['John Smith'], ['a@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['John']


def test_fixrecords_capitalizes_first_letter_and_lowercases_rest():
    df = _records(['ALLCAPS', 'mary'], ['a@x.com', 'b@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Allcaps', 'Mary']


def test_fixrecords_non_ascii_name_becomes_colleague():
    df = _records(['José'], ['a@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Colleague']


def test_fixrecords_leading_whitespace_in_name_yields_empty_string():
    # Documents current behavior, doesn't endorse it: the code splits on
    # ' ' and takes the first token *before* stripping, so a name with a
    # leading space produces an empty token rather than 'Colleague' or the
    # trimmed name.
    df = _records([' Leadingspace'], ['a@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['']


def test_fixrecords_trailing_whitespace_in_name_is_stripped():
    df = _records(['Mary  '], ['a@x.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Mary']


def test_fixrecords_email_is_lowercased_and_stripped():
    df = _records(['Mary'], ['  Mary@EXAMPLE.com  '])
    result = list_.FixRecords(df)
    assert result['email'].tolist() == ['mary@example.com']


def test_fixrecords_drops_rows_with_missing_email():
    df = _records(['Mary', 'John'], ['a@x.com', None])
    result = list_.FixRecords(df)
    assert result['email'].tolist() == ['a@x.com']


def test_fixrecords_drops_duplicate_emails_keeping_first_occurrence():
    df = _records(['Mary', 'John'], ['a@x.com', 'A@X.COM'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Mary']
    assert result['email'].tolist() == ['a@x.com']


def test_fixrecords_drops_malformed_emails():
    df = _records(['Mary', 'Anna'], ['mary@example.com', 'not-an-email'])
    result = list_.FixRecords(df)
    assert result['email'].tolist() == ['mary@example.com']


def test_fixrecords_well_formed_row_passes_through_unchanged():
    df = _records(['Mary'], ['mary@example.com'])
    result = list_.FixRecords(df)
    assert result['first_name'].tolist() == ['Mary']
    assert result['email'].tolist() == ['mary@example.com']
```

- [ ] **Step 2: Run the tests and confirm they pass**

Run: `pytest tests/test_lists.py -v`
Expected: all tests in this file pass (no production code changes needed — this locks in existing behavior).

- [ ] **Step 3: Commit**

```bash
git add tests/test_lists.py
git commit -m "Add FixRecords regression tests"
```

---

### Task 3: `fix_columns` tests

**Files:**
- Create: `tests/fixtures/sample_mappers.json`
- Modify: `tests/test_lists.py`

**Interfaces:**
- Consumes: `fake_constants.FILE_COLUMNS_DICT_PATH` (Task 1), which already defaults to `tests/fixtures/sample_mappers.json` — no monkeypatching needed as long as this file exists at that path.
- Consumes: `modules.lists.list_.fix_columns(df: pd.DataFrame) -> pd.DataFrame | None`, called as `list_.fix_columns(df)`.
- Produces: `tests/fixtures/sample_mappers.json` with `apollo_test`, `hunter_test`, `ru_test`, `snov_test` mapper entries — consumed again by Task 6.

- [ ] **Step 1: Add the mapper fixture**

Create `tests/fixtures/sample_mappers.json`:

```json
{
    "mappers": [
        {
            "name": "apollo_test",
            "map": {
                "First Name": "first_name",
                "Email": "email"
            }
        },
        {
            "name": "hunter_test",
            "map": {
                "Full Name": "first_name",
                "Email Address": "email"
            }
        },
        {
            "name": "ru_test",
            "map": {
                "Name": "first_name",
                "E-mail": "email"
            }
        },
        {
            "name": "snov_test",
            "map": {
                "Contact Name": "first_name",
                "Contact Email": "email"
            }
        }
    ]
}
```

- [ ] **Step 2: Write the tests**

Append to `tests/test_lists.py`:

```python
def test_fix_columns_renames_and_reorders_matching_source():
    # Columns deliberately out of order vs. the mapper's key order, to
    # prove fix_columns reorders to match the mapper's value order too.
    df = pd.DataFrame({'Email': ['JOHN@X.COM'], 'First Name': ['John Doe']})
    result = list_.fix_columns(df)
    assert result.columns.tolist() == ['first_name', 'email']
    assert result['first_name'].tolist() == ['John Doe']
    assert result['email'].tolist() == ['JOHN@X.COM']


def test_fix_columns_returns_none_when_no_mapper_matches_and_choice_invalid(monkeypatch):
    df = pd.DataFrame({'Weird Column': ['x'], 'Another Col': ['y']})
    monkeypatch.setattr('builtins.input', lambda *args, **kwargs: 'nope')
    result = list_.fix_columns(df)
    assert result is None
```

- [ ] **Step 3: Run the tests and confirm they pass**

Run: `pytest tests/test_lists.py -v`
Expected: all tests pass, including the two new ones.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/sample_mappers.json tests/test_lists.py
git commit -m "Add fix_columns regression tests and mapper fixture"
```

---

### Task 4: `CleanBlacklisted` tests

**Files:**
- Create: `tests/fixtures/sample_blacklist.txt`
- Modify: `tests/test_lists.py`

**Interfaces:**
- Consumes: `fake_constants.BLACKLIST_PATH` (Task 1), defaults to `tests/fixtures/sample_blacklist.txt` — no monkeypatching needed.
- Consumes: `modules.lists.list_.CleanBlacklisted(df: pd.DataFrame) -> pd.DataFrame`, called as `list_.CleanBlacklisted(df)`.

- [ ] **Step 1: Add the blacklist fixture**

Create `tests/fixtures/sample_blacklist.txt`:

```
blocked.com
banned@example.com
```

- [ ] **Step 2: Write the test**

Append to `tests/test_lists.py`:

```python
def test_clean_blacklisted_removes_matching_emails_and_keeps_others():
    df = _records(
        ['A', 'B', 'C', 'D'],
        ['user@blocked.com', 'ok@good.com', 'banned@example.com', 'fine@site.com'],
    )
    result = list_.CleanBlacklisted(df)
    assert result['email'].tolist() == ['ok@good.com', 'fine@site.com']
```

- [ ] **Step 3: Run the tests and confirm they pass**

Run: `pytest tests/test_lists.py -v`
Expected: all tests pass, including the new one.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/sample_blacklist.txt tests/test_lists.py
git commit -m "Add CleanBlacklisted regression test and blacklist fixture"
```

---

### Task 5: `modules/utilities.py` tests

**Files:**
- Create: `tests/test_utilities.py`

**Interfaces:**
- Consumes: `modules.utilities.find_matching_columns(keywords: list[str], current_column_names: list[str]) -> list[str]`.
- Consumes: `modules.utilities.add_new_column_mapper(mapper_name: str, mapper_values: dict) -> None` (writes to `const.FILE_COLUMNS_DICT_PATH`).
- Consumes: `tests.fixtures.fake_constants` (Task 1) directly, to monkeypatch `FILE_COLUMNS_DICT_PATH` to a `tmp_path` file for the write test — this must NOT point at the shared `tests/fixtures/sample_mappers.json`, since that file is a committed fixture other tasks read and must not be mutated by a test run.

- [ ] **Step 1: Write the tests**

Create `tests/test_utilities.py`:

```python
import json

from modules.utilities import add_new_column_mapper, find_matching_columns
from tests.fixtures import fake_constants


def test_find_matching_columns_lowercases_keywords_before_matching():
    columns = ['first_name', 'email_address', 'company']
    result = find_matching_columns(['First', 'EMAIL'], columns)
    assert result == ['first_name', 'email_address']


def test_find_matching_columns_is_case_sensitive_on_column_names():
    # Documents current behavior: keywords are lowercased before matching,
    # but column names are matched as-is, so a mixed-case column name
    # won't match unless the matching substring happens to be lowercase.
    columns = ['first_name', 'Email_Address', 'company']
    result = find_matching_columns(['first', 'email'], columns)
    assert result == ['first_name']


def test_find_matching_columns_no_match_returns_empty_list():
    result = find_matching_columns(['zzz'], ['first_name', 'email_address'])
    assert result == []


def test_add_new_column_mapper_appends_and_persists(tmp_path, monkeypatch):
    mapper_file = tmp_path / 'mappers.json'
    mapper_file.write_text(json.dumps({
        'mappers': [{'name': 'existing', 'map': {'A': 'a'}}],
    }))
    monkeypatch.setattr(fake_constants, 'FILE_COLUMNS_DICT_PATH', mapper_file)

    add_new_column_mapper('new_mapper', {'Raw Col': 'clean_col'})

    saved = json.loads(mapper_file.read_text())
    assert saved['mappers'][0]['name'] == 'existing'
    assert saved['mappers'][1] == {
        'name': 'new_mapper',
        'map': {'Raw Col': 'clean_col'},
    }
```

- [ ] **Step 2: Run the tests and confirm they pass**

Run: `pytest tests/test_utilities.py -v`
Expected: `4 passed`

- [ ] **Step 3: Commit**

```bash
git add tests/test_utilities.py
git commit -m "Add tests for modules/utilities.py column-mapper helpers"
```

---

### Task 6: Per-source pipeline tests (`fix_columns` → `FixRecords`)

**Files:**
- Create: `tests/fixtures/raw_apollo.csv`
- Create: `tests/fixtures/raw_hunter.csv`
- Create: `tests/fixtures/raw_ru.csv`
- Create: `tests/fixtures/raw_snov.csv`
- Modify: `tests/test_lists.py`

**Interfaces:**
- Consumes: `tests/fixtures/sample_mappers.json` (Task 3) — must already contain `apollo_test`/`hunter_test`/`ru_test`/`snov_test` with the exact header names used in the CSVs below.
- Consumes: `modules.lists.list_.ReadList(file_path) -> pd.DataFrame`, `list_.fix_columns`, `list_.FixRecords` (all called unbound on `list_`).

Each CSV uses source-specific headers but the identical row pattern, so one source's headers can't accidentally match another source's mapper:

- [ ] **Step 1: Add the four raw CSV fixtures**

Create `tests/fixtures/raw_apollo.csv`:

```csv
First Name,Email
John Smith,JOHN@EXAMPLE.COM
Mary,mary@example.com
Mary,MARY@EXAMPLE.COM
José,jose@example.com
Anna,not-an-email
Paul,
```

Create `tests/fixtures/raw_hunter.csv`:

```csv
Full Name,Email Address
John Smith,JOHN@EXAMPLE.COM
Mary,mary@example.com
Mary,MARY@EXAMPLE.COM
José,jose@example.com
Anna,not-an-email
Paul,
```

Create `tests/fixtures/raw_ru.csv`:

```csv
Name,E-mail
John Smith,JOHN@EXAMPLE.COM
Mary,mary@example.com
Mary,MARY@EXAMPLE.COM
José,jose@example.com
Anna,not-an-email
Paul,
```

Create `tests/fixtures/raw_snov.csv`:

```csv
Contact Name,Contact Email
John Smith,JOHN@EXAMPLE.COM
Mary,mary@example.com
Mary,MARY@EXAMPLE.COM
José,jose@example.com
Anna,not-an-email
Paul,
```

Note: `list_.ReadList` reads these with `encoding='latin-1'`. If a fixture file
ends up saved as UTF-8 on disk, the "é" byte sequence will decode into two
different non-ASCII characters rather than "é" itself — that's fine and
expected: the assertion below only checks that the row's first name ends up
as `'Colleague'`, which holds either way since the decoded result is still
non-ASCII.

- [ ] **Step 2: Write the parametrized pipeline test**

Append to `tests/test_lists.py`:

```python
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / 'fixtures'

SOURCE_CSVS = ['raw_apollo.csv', 'raw_hunter.csv', 'raw_ru.csv', 'raw_snov.csv']


@pytest.mark.parametrize('csv_name', SOURCE_CSVS)
def test_fix_columns_then_fixrecords_cleans_each_source(csv_name):
    df = list_.ReadList(FIXTURES_DIR / csv_name)

    df = list_.fix_columns(df)
    assert df.columns.tolist() == ['first_name', 'email']

    df = list_.FixRecords(df)
    # Row 1: passes through ("John Smith" -> "John"). Row 2: passes
    # through. Row 3: dropped, a case-insensitive duplicate of row 2's
    # email. Row 4: passes through, non-ascii name -> "Colleague". Row 5:
    # dropped, malformed email. Row 6: dropped, missing email.
    assert df['first_name'].tolist() == ['John', 'Mary', 'Colleague']
    assert df['email'].tolist() == [
        'john@example.com', 'mary@example.com', 'jose@example.com',
    ]
```

- [ ] **Step 3: Run the tests and confirm they pass**

Run: `pytest tests/test_lists.py -v`
Expected: all tests pass, including 4 new parametrized cases (one per source).

- [ ] **Step 4: Run the full suite**

Run: `pytest -v`
Expected: every test collected across `tests/` passes.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/raw_apollo.csv tests/fixtures/raw_hunter.csv \
        tests/fixtures/raw_ru.csv tests/fixtures/raw_snov.csv \
        tests/test_lists.py
git commit -m "Add per-source fix_columns -> FixRecords pipeline tests"
```

---

### Task 7: Docs follow-through

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

**Interfaces:** None — documentation only.

- [ ] **Step 1: Update README.md**

In the `## Running` section, replace:

```markdown
There is no formal test suite — testing is done interactively in `notes.ipynb`.
```

with:

```markdown
Day-to-day exploratory testing still happens interactively in `notes.ipynb`.
Automated regression tests live under `tests/` — see **Testing** below.
```

Then add a new `## Testing` section right after `## Running` (before `## Architecture`):

```markdown
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
```
```

- [ ] **Step 2: Update CLAUDE.md**

In the `## Running the program` section, replace:

```markdown
There is no formal test suite. Testing is done interactively in `notes.ipynb`.
```

with:

```markdown
Automated regression tests for the list-cleaning/mapper logic in
`modules/lists.py` and `modules/utilities.py` live under `tests/` — run with
`pytest` after `pip install -r requirements-dev.txt` (see README's Testing
section). Day-to-day exploratory testing of everything else still happens
interactively in `notes.ipynb`.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "Document the new pytest suite in README and CLAUDE.md"
```
