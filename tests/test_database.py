from modules.database import Database
from tests.fixtures import fake_constants


def test_update_blocked_emails_concurrency_adds_lowercased_new_entries(tmp_path, monkeypatch):
    blacklist_file = tmp_path / 'blacklist.txt'
    blacklist_file.write_text('blocked.com\nbanned@example.com\n')
    monkeypatch.setattr(fake_constants, 'BLACKLIST_PATH', blacklist_file)

    # Regression test for a crash: writing back with to_csv(sep='\n')
    # raises "ValueError: bad delimiter value" because Python's csv module
    # rejects newline as a delimiter. The file has one column, so no
    # delimiter is ever written between fields -- the default sep is fine.
    Database().update_blocked_emails_concurrency('NEW@Example.com,Banned@Example.com')

    saved = blacklist_file.read_text().splitlines()
    assert saved == ['blocked.com', 'banned@example.com', 'new@example.com']


def test_update_blocked_emails_concurrency_no_new_entries(tmp_path, monkeypatch, capsys):
    blacklist_file = tmp_path / 'blacklist.txt'
    blacklist_file.write_text('banned@example.com\n')
    monkeypatch.setattr(fake_constants, 'BLACKLIST_PATH', blacklist_file)

    Database().update_blocked_emails_concurrency('Banned@Example.com')

    assert blacklist_file.read_text().splitlines() == ['banned@example.com']
    assert 'No new records were added' in capsys.readouterr().out
