"""Shared test fixtures."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from pydantic import BaseModel

from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore


# ---------------------------------------------------------------------------
# Example Pydantic schemas used across tests
# ---------------------------------------------------------------------------


class ImageRecord(BaseModel):
    image_id: str
    timestamp: datetime
    instrument: str
    year: int
    month: int


class ClassificationRecord(BaseModel):
    image_id: str
    model_version: str
    class_name: str
    score: float
    is_winner: bool
    instrument: str
    year: int
    month: int


class RecordWithOptional(BaseModel):
    image_id: str
    instrument: str
    year: int
    month: int
    notes: Optional[str] = None


class RecordWithJsonBlob(BaseModel):
    image_id: str
    instrument: str
    year: int
    month: int
    data: dict


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


def make_image(
    image_id: str = "D20240101T120000_IFCB107_00001",
    instrument: str = "IFCB107",
    year: int = 2024,
    month: int = 1,
) -> dict:
    return {
        "image_id": image_id,
        "timestamp": datetime(year, month, 1, 12, 0, 0, tzinfo=timezone.utc),
        "instrument": instrument,
        "year": year,
        "month": month,
    }


def make_classification(
    image_id: str = "D20240101T120000_IFCB107_00001",
    model_version: str = "cnn-v1",
    class_name: str = "Ceratium",
    score: float = 0.92,
    is_winner: bool = True,
    instrument: str = "IFCB107",
    year: int = 2024,
    month: int = 1,
) -> dict:
    return {
        "image_id": image_id,
        "model_version": model_version,
        "class_name": class_name,
        "score": score,
        "is_winner": is_winner,
        "instrument": instrument,
        "year": year,
        "month": month,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path) -> DuckDBParquetStore:
    """An empty DuckDBParquetStore backed by a temp directory."""
    config = DuckDBParquetConfig(root=str(tmp_path))
    return DuckDBParquetStore(config)


@pytest.fixture
def image_store(store: DuckDBParquetStore) -> DuckDBParquetStore:
    """A store with the 'images' table already created."""
    store.create_table(
        "images",
        ImageRecord,
        partition_by=["instrument", "year", "month"],
    )
    return store
