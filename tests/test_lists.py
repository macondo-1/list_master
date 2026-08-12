from pathlib import Path

import pandas as pd
import pytest

from modules.lists import list_

FIXTURES_DIR = Path(__file__).parent / 'fixtures'

SOURCE_CSVS = ['raw_apollo.csv', 'raw_hunter.csv', 'raw_ru.csv', 'raw_snov.csv']


def _records(first_names, emails):
    # dtype=object avoids an all-None/NaN column inferring float64 (breaks
    # .str accessors below); matches ReadList's own dtype=object.
    return pd.DataFrame({'first_name': first_names, 'email': emails}, dtype=object)


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


def test_fix_columns_returns_none_when_matched_mapper_is_missing_a_column():
    # A single-column df is a subset of apollo_test's {'First Name', 'Email'}
    # keys, so the isin().all() check matches it — but the mapper's rename
    # target list still expects both columns, so selecting by the mapper's
    # values raises and the function's bare except returns None.
    df = pd.DataFrame({'First Name': ['John']})
    result = list_.fix_columns(df)
    assert result is None


def test_clean_blacklisted_removes_matching_emails_and_keeps_others():
    df = _records(
        ['A', 'B', 'C', 'D'],
        ['user@blocked.com', 'ok@good.com', 'banned@example.com', 'fine@site.com'],
    )
    result = list_.CleanBlacklisted(df)
    assert result['email'].tolist() == ['ok@good.com', 'fine@site.com']


def test_clean_blacklisted_is_case_sensitive_on_email_values():
    # Documents current behavior: blacklist file lines are lowercased before
    # building the regex, but df.email is matched as-is (no case=False), so
    # an uppercase email matching a lowercase blacklist entry is NOT caught.
    # In production this is masked because FixRecords always lowercases
    # email before CleanBlacklisted runs — but CleanBlacklisted itself does
    # not enforce that.
    df = _records(['A'], ['USER@BLOCKED.COM'])
    result = list_.CleanBlacklisted(df)
    assert result['email'].tolist() == ['USER@BLOCKED.COM']


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
