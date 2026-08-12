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
