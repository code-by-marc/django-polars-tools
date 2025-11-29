import polars as pl
import pytest


def test_hypothesis() -> None:
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        pl.DataFrame(
            [{"name": None}, {"name": "John Doe"}],
            infer_schema_length=1,
        )

    assert "infer_schema_length" in str(excinfo.value)
