import pandas as pd

from modules.lists import list_


def _records(first_names, emails):
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
