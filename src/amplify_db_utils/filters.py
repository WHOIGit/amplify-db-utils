"""Filter dict → SQL WHERE clause translation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amplify_db_utils.base import Filters

# Supported range operators and their SQL equivalents
_RANGE_OPS = {
    "gte": ">=",
    "gt": ">",
    "lte": "<=",
    "lt": "<",
}


def filters_to_sql(filters: "Filters | None", params: list) -> str:
    """Translate a filter dict to a SQL WHERE clause fragment.

    Returns the clause text WITHOUT the ``WHERE`` keyword. Empty or ``None``
    filters return ``"1=1"`` to keep query structure uniform.

    All user values are appended to ``params`` as positional parameters
    (DuckDB ``$N`` style) — no string interpolation of user values.

    Args:
        filters: Filter dict or None. Supports:
            - Equality: ``{"field": value}``
            - Range: ``{"field": {"gte": x, "lt": y}}``
            - Set: ``{"field": {"in": [a, b, c]}}``
        params: List to append parameter values to (mutated in place).

    Returns:
        SQL fragment, e.g. ``'instrument = $1 AND year >= $2 AND year < $3'``.
    """
    if not filters:
        return "1=1"

    clauses: list[str] = []

    for field, value in filters.items():
        quoted = f'"{field}"'

        if isinstance(value, dict):
            if "in" in value:
                items = value["in"]
                if not items:
                    # Empty IN list — no rows can match
                    clauses.append("1=0")
                    continue
                placeholders = []
                for item in items:
                    params.append(item)
                    placeholders.append(f"${len(params)}")
                clauses.append(f"{quoted} IN ({', '.join(placeholders)})")
            else:
                # Range operators — emit only the keys present
                for op, sql_op in _RANGE_OPS.items():
                    if op in value:
                        params.append(value[op])
                        clauses.append(f"{quoted} {sql_op} ${len(params)}")
        else:
            params.append(value)
            clauses.append(f"{quoted} = ${len(params)}")

    return " AND ".join(clauses) if clauses else "1=1"
