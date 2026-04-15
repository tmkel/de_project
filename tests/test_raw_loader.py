from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.storage import raw_loader


NATIONAL_INTENSITY_ROWS = pd.DataFrame(
    [
        {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "intensity.forecast": 195,
            "intensity.actual": 193,
            "intensity.index": "moderate",
        },
        {
            "from": "2022-01-01T00:30Z",
            "to": "2022-01-01T01:00Z",
            "intensity.forecast": 200,
            "intensity.actual": 198,
            "intensity.index": "moderate",
        },
    ]
)

GENERATION_ROWS = pd.DataFrame(
    [
        {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "fuel": "wind",
            "perc": 25.0,
        },
        {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "fuel": "gas",
            "perc": 35.2,
        },
    ]
)

REGIONAL_ROWS = pd.DataFrame(
    [
        {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "regionid": 1,
            "dnoregion": "Scottish Hydro Electric Power Distribution",
            "shortname": "North Scotland",
            "intensity.forecast": 42,
            "intensity.index": "very low",
            "fuel": "wind",
            "percentage": 80.0,
        }
    ]
)


class TestStagedFileExists:
    def test_returns_true_when_parquet_exists(self, tmp_path, monkeypatch):
        staging_dir = tmp_path / "data/staging/national_intensity"
        staging_dir.mkdir(parents=True)
        (staging_dir / "2022-01-01.parquet").write_bytes(b"")
        monkeypatch.chdir(tmp_path)

        assert raw_loader.staged_file_exists("national_intensity", "2022-01-01") is True

    def test_returns_false_when_parquet_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert raw_loader.staged_file_exists("national_intensity", "2022-01-01") is False


class TestLoadRawNationalIntensity:
    def test_builds_tuple_rows_and_calls_executemany(self, tmp_path, monkeypatch):
        staging_dir = tmp_path / "data/staging/national_intensity"
        staging_dir.mkdir(parents=True)
        NATIONAL_INTENSITY_ROWS.to_parquet(staging_dir / "2022-01-01.parquet")
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        raw_loader.load_raw_national_intensity(cursor, "2022-01-01")

        cursor.executemany.assert_called_once()
        _, rows = cursor.executemany.call_args.args
        assert rows == [
            ("2022-01-01T00:00Z", "2022-01-01T00:30Z", 195, 193, "moderate"),
            ("2022-01-01T00:30Z", "2022-01-01T01:00Z", 200, 198, "moderate"),
        ]


class TestLoadRawGeneration:
    def test_builds_tuple_rows_and_calls_executemany(self, tmp_path, monkeypatch):
        staging_dir = tmp_path / "data/staging/generation"
        staging_dir.mkdir(parents=True)
        GENERATION_ROWS.to_parquet(staging_dir / "2022-01-01.parquet")
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        raw_loader.load_raw_generation(cursor, "2022-01-01")

        cursor.executemany.assert_called_once()
        _, rows = cursor.executemany.call_args.args
        assert rows == [
            ("2022-01-01T00:00Z", "2022-01-01T00:30Z", "wind", 25.0),
            ("2022-01-01T00:00Z", "2022-01-01T00:30Z", "gas", 35.2),
        ]


class TestLoadRawRegionalIntensity:
    def test_builds_tuple_rows_and_calls_executemany(self, tmp_path, monkeypatch):
        staging_dir = tmp_path / "data/staging/regional_intensity"
        staging_dir.mkdir(parents=True)
        REGIONAL_ROWS.to_parquet(staging_dir / "2022-01-01.parquet")
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        raw_loader.load_raw_regional_intensity(cursor, "2022-01-01")

        cursor.executemany.assert_called_once()
        _, rows = cursor.executemany.call_args.args
        assert rows == [
            (
                "2022-01-01T00:00Z",
                "2022-01-01T00:30Z",
                1,
                "Scottish Hydro Electric Power Distribution",
                "North Scotland",
                42,
                "very low",
                "wind",
                80.0,
            )
        ]


class TestLoadRawDate:
    def test_deletes_then_inserts_per_dataset_and_commits(self, tmp_path, monkeypatch):
        for dataset, df in (
            ("national_intensity", NATIONAL_INTENSITY_ROWS),
            ("generation", GENERATION_ROWS),
            ("regional_intensity", REGIONAL_ROWS),
        ):
            staging_dir = tmp_path / f"data/staging/{dataset}"
            staging_dir.mkdir(parents=True)
            df.to_parquet(staging_dir / "2022-01-01.parquet")
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(raw_loader, "get_connection", lambda: connection)

        raw_loader.load_raw_date("2022-01-01")

        delete_statements = [
            call.args[0]
            for call in cursor.execute.call_args_list
            if "DELETE" in call.args[0]
        ]
        assert len(delete_statements) == 3
        assert cursor.executemany.call_count == 3
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()

    def test_rolls_back_on_insert_failure(self, tmp_path, monkeypatch):
        staging_dir = tmp_path / "data/staging/national_intensity"
        staging_dir.mkdir(parents=True)
        NATIONAL_INTENSITY_ROWS.to_parquet(staging_dir / "2022-01-01.parquet")
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        cursor.executemany.side_effect = RuntimeError("boom")
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(raw_loader, "get_connection", lambda: connection)

        raw_loader.load_raw_date("2022-01-01")

        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()

    def test_skips_missing_staged_datasets(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(raw_loader, "get_connection", lambda: connection)

        raw_loader.load_raw_date("2022-01-01")

        cursor.execute.assert_not_called()
        cursor.executemany.assert_not_called()
        connection.commit.assert_called_once()

    def test_returns_early_when_connection_fails(self, monkeypatch):
        monkeypatch.setattr(raw_loader, "get_connection", lambda: None)

        raw_loader.load_raw_date("2022-01-01")
