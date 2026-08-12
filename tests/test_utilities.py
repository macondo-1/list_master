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
