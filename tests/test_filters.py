"""Unit tests for filters_to_sql."""

import pytest

from amplify_db_utils.filters import filters_to_sql


def _sql_and_params(filters):
    params = []
    sql = filters_to_sql(filters, params)
    return sql, params


def test_none_filters_returns_true():
    sql, params = _sql_and_params(None)
    assert sql == "1=1"
    assert params == []


def test_empty_dict_returns_true():
    sql, params = _sql_and_params({})
    assert sql == "1=1"
    assert params == []


def test_equality_string():
    sql, params = _sql_and_params({"instrument": "IFCB107"})
    assert sql == '"instrument" = $1'
    assert params == ["IFCB107"]


def test_equality_int():
    sql, params = _sql_and_params({"year": 2024})
    assert sql == '"year" = $1'
    assert params == [2024]


def test_equality_float():
    sql, params = _sql_and_params({"score": 0.9})
    assert '"score" = $1' in sql
    assert params == [0.9]


def test_equality_bool():
    sql, params = _sql_and_params({"is_winner": True})
    assert '"is_winner" = $1' in sql
    assert params == [True]


def test_range_gte_lt():
    sql, params = _sql_and_params({"timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"}})
    assert '"timestamp" >= $1' in sql
    assert '"timestamp" < $2' in sql
    assert params == ["2024-01-01", "2024-02-01"]


def test_range_single_operator():
    sql, params = _sql_and_params({"score": {"gte": 0.8}})
    assert '"score" >= $1' in sql
    assert params == [0.8]


def test_range_all_operators():
    sql, params = _sql_and_params({"val": {"gt": 1, "gte": 2, "lt": 10, "lte": 9}})
    assert '"val" > $' in sql
    assert '"val" >= $' in sql
    assert '"val" < $' in sql
    assert '"val" <= $' in sql
    assert len(params) == 4


def test_in_filter():
    sql, params = _sql_and_params({"kind": {"in": ["blob", "features"]}})
    assert '"kind" IN ($1, $2)' in sql
    assert params == ["blob", "features"]


def test_in_filter_single():
    sql, params = _sql_and_params({"kind": {"in": ["blob"]}})
    assert '"kind" IN ($1)' in sql
    assert params == ["blob"]


def test_in_filter_empty_list():
    sql, params = _sql_and_params({"kind": {"in": []}})
    assert "1=0" in sql
    assert params == []


def test_multiple_equality_fields():
    sql, params = _sql_and_params({"instrument": "IFCB107", "year": 2024})
    assert '"instrument" = $1' in sql
    assert '"year" = $2' in sql
    assert params == ["IFCB107", 2024]


def test_combined_equality_and_range():
    sql, params = _sql_and_params({
        "instrument": "IFCB107",
        "timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"},
    })
    assert '"instrument" = $1' in sql
    assert '"timestamp" >= $2' in sql
    assert '"timestamp" < $3' in sql
    assert params == ["IFCB107", "2024-01-01", "2024-02-01"]


def test_param_numbering_is_sequential():
    """Parameter positions must increment correctly across multiple filters."""
    params = []
    sql = filters_to_sql({"a": 1, "b": 2, "c": 3}, params)
    assert params == [1, 2, 3]
    assert "$1" in sql
    assert "$2" in sql
    assert "$3" in sql


def test_params_appended_to_existing_list():
    """filters_to_sql appends to params, picking up numbering where it left off."""
    params = ["existing"]  # $1 already used
    sql = filters_to_sql({"x": 42}, params)
    assert "$2" in sql
    assert params == ["existing", 42]
