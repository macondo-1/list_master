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
