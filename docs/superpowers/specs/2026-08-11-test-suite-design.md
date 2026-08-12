# Test suite for list_master

Date: 2026-08-11
Status: Approved

## Goal

Add a pytest suite that guards the list-cleaning and column-mapper logic in
`modules/lists.py` and `modules/utilities.py` against regressions. This is the
pure, deterministic logic in the codebase — no Selenium, no real SMTP, no live
GreenArrow/Email Bison/SurveyMonkey — and therefore the cheapest to test and the
most likely to silently break without anyone noticing.

## Non-goals

Explicitly out of scope for this pass:

- `modules/database.py` (SQLite queries)
- `modules/greenarrow_bot.py`, `modules/bcc_bot.py` (Selenium)
- `modules/smtp_bot.py` (real SMTP sends)
- `modules/sm_api.py`, `modules/email_bison_api/` (external APIs)
- `modules/display.py`
- The legacy root-level scripts (`lists.py`, `greenarrow_bot.py`, etc.) — `modules/`
  is the active codebase per `CLAUDE.md`; the root scripts are kept for reference
  only.
- Interactive, `input()`-driven code paths: `list_.clean_list_manually`,
  `create_new_column_mapper`. These are human-in-the-loop by design; a bug there
  is caught immediately by the operator at runtime, so the cost/benefit of
  mocking a chain of `input()` calls isn't worth it right now.
- The deprecated `list_.FixColumns` / `list_.FixUnknownColumns` (superseded by
  `fix_columns`, per the code's own comments).

## The constants problem

`modules/lists.py` and `modules/utilities.py` both do
`import modules.constants as const` at module load time. `modules/constants.py`
is gitignored and machine-specific (real Google Drive paths, credentials,
mapper dicts for the deprecated path) — it doesn't exist on a fresh checkout or
in CI, so `import modules.lists` fails before any test can run.

Fix: a root-level `conftest.py` swaps in a tracked fake module before anything
imports `modules.lists` or `modules.utilities`:

```python
# conftest.py (repo root)
import sys
from tests.fixtures import fake_constants
sys.modules['modules.constants'] = fake_constants
```

`tests/fixtures/fake_constants.py` defines dummy `Path` values for every
attribute `lists.py`/`utilities.py` touch in the functions under test
(`FILE_COLUMNS_DICT_PATH`, `BLACKLIST_PATH`, `DB_COLUMNS`). No real Drive paths,
no credentials, nothing copied from the real `constants.py`.

Individual tests override specific attributes with
`monkeypatch.setattr(fake_constants, "FILE_COLUMNS_DICT_PATH", tmp_path / "mappers.json")`
to point at per-test fixture data — this works because `const` inside
`modules.lists`/`modules.utilities` and `fake_constants` are the same module
object once the `sys.modules` swap has run.

This root `conftest.py` also solves package resolution: there's no
`__init__.py` anywhere in the repo and no installed package, so plain `pytest`
needs a conftest.py at the repo root for its "prepend to sys.path" basedir
logic to add the repo root (rather than `tests/`) to `sys.path`. Without it,
`import modules.lists` from inside `tests/test_lists.py` would fail even with
the constants shim in place.

## Coverage

| File | Function | Cases |
|---|---|---|
| `modules/lists.py` | `FixRecords` | missing first name → `Colleague`; multi-word first name → first token; leading/trailing whitespace stripped; non-ascii first name → `Colleague`; email lowercased + stripped; duplicate emails dropped (keep first); rows with missing email dropped; malformed emails filtered out; well-formed rows pass through unchanged |
| | `fix_columns` | raw columns exactly match a mapper's keys in the JSON registry → columns renamed and reordered to the mapper's values; no mapper matches and the interactive fallback is given an invalid choice → returns `None` |
| | `CleanBlacklisted` | rows whose email matches a blacklist entry (substring, case-insensitive) are removed; non-matching rows are kept |
| `modules/utilities.py` | `find_matching_columns` | keyword matching is case-insensitive; only columns containing a keyword are returned; no match → empty list |
| | `add_new_column_mapper` | appends a `{name, map}` entry to the JSON registry's `"mappers"` list and persists it to disk (verified by re-reading the file) |

## Fixtures

`tests/fixtures/` holds small, synthetic (non-client) data:

- `raw_apollo.csv`, `raw_hunter.csv`, `raw_ru.csv`, `raw_snov.csv` — a handful of
  rows each, mimicking each source's real export headers, with deliberate edge
  cases baked in: one duplicate email, one row with a missing email, one row
  with a malformed email, one non-ascii first name.
- `sample_mappers.json` — matches the real registry shape
  (`{"mappers": [{"name": ..., "map": {...}}]}`) with entries for the four
  sample sources above.
- `sample_blacklist.txt` — a couple of synthetic blocked email substrings.
- `fake_constants.py` — the constants test double described above.

None of this is copied from the real, gitignored `modules/constants.py` or
`utilities/mappers.json` — it's written fresh to avoid any chance of carrying
over real client column-mapper names.

## Layout

```
conftest.py                    # constants shim + sys.path bootstrap
pytest.ini                     # testpaths = tests
requirements-dev.txt           # -r requirements.txt, pytest
tests/
  test_lists.py
  test_utilities.py
  fixtures/
    fake_constants.py
    sample_mappers.json
    sample_blacklist.txt
    raw_apollo.csv
    raw_hunter.csv
    raw_ru.csv
    raw_snov.csv
```

## Running

```bash
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

## Docs follow-through

- `README.md`: replace "There is no formal test suite" with a short "Running
  tests" section pointing at the above.
- `CLAUDE.md`: update the same note so future sessions know the suite exists
  and where it lives.
