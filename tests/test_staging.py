import json
from unittest.mock import patch

import pandas as pd

from src.storage import staging


SAMPLE_NATIONAL_INTENSITY = [
    {
        "from": "2022-01-01T00:00Z",
        "to": "2022-01-01T00:30Z",
        "intensity": {"forecast": 195, "actual": 193, "index": "moderate"},
    }
]

SAMPLE_GENERATION = [
    {
        "from": "2022-01-01T00:00Z",
        "to": "2022-01-01T00:30Z",
        "generationmix": [
            {"fuel": "wind", "perc": 25.0},
            {"fuel": "gas", "perc": 35.2},
        ],
    }
]

SAMPLE_REGIONAL_INTENSITY = [
    {
        "from": "2022-01-01T00:00Z",
        "to": "2022-01-01T00:30Z",
        "regions": [
            {
                "regionid": 1,
                "dnoregion": "Scottish Hydro Electric Power Distribution",
                "shortname": "North Scotland",
                "intensity": {"forecast": 42, "index": "very low"},
                "generationmix": [
                    {"fuel": "wind", "perc": 80.0},
                    {"fuel": "hydro", "perc": 15.0},
                ],
            }
        ],
    }
]


def _write_json(base_path, relative_path, payload):
    file_path = base_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload))
    return file_path


def _read_parquet(base_path, relative_path):
    return pd.read_parquet(base_path / relative_path)


class TestLoadJson:
    def test_load_json_reads_file_contents(self, tmp_path):
        file_path = _write_json(
            tmp_path,
            "data/raw/national_intensity/2022-01-01.json",
            SAMPLE_NATIONAL_INTENSITY,
        )

        result = staging.load_json(str(file_path))

        assert result == SAMPLE_NATIONAL_INTENSITY


class TestStageNationalIntensity:
    def test_stage_national_intensity_writes_parquet(self, tmp_path, monkeypatch):
        _write_json(
            tmp_path,
            "data/raw/national_intensity/2022-01-01.json",
            SAMPLE_NATIONAL_INTENSITY,
        )
        (tmp_path / "data/staging/national_intensity").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_national_intensity("2022-01-01")

        result = _read_parquet(
            tmp_path, "data/staging/national_intensity/2022-01-01.parquet"
        )
        assert len(result) == 1
        assert result.loc[0, "from"] == "2022-01-01T00:00Z"
        assert result.loc[0, "intensity.forecast"] == 195
        assert result.loc[0, "intensity.actual"] == 193
        assert result.loc[0, "intensity.index"] == "moderate"

    def test_stage_national_intensity_skips_empty_input(self, tmp_path, monkeypatch):
        _write_json(tmp_path, "data/raw/national_intensity/2022-01-01.json", [])
        (tmp_path / "data/staging/national_intensity").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_national_intensity("2022-01-01")

        assert not (
            tmp_path / "data/staging/national_intensity/2022-01-01.parquet"
        ).exists()


class TestStageGeneration:
    def test_stage_generation_writes_flattened_parquet(self, tmp_path, monkeypatch):
        _write_json(tmp_path, "data/raw/generation/2022-01-01.json", SAMPLE_GENERATION)
        (tmp_path / "data/staging/generation").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_generation("2022-01-01")

        result = _read_parquet(tmp_path, "data/staging/generation/2022-01-01.parquet")
        assert len(result) == 2
        assert set(result["fuel"]) == {"wind", "gas"}
        assert set(result["perc"]) == {25.0, 35.2}
        assert set(result["from"]) == {"2022-01-01T00:00Z"}
        assert set(result["to"]) == {"2022-01-01T00:30Z"}

    def test_stage_generation_skips_empty_input(self, tmp_path, monkeypatch):
        _write_json(tmp_path, "data/raw/generation/2022-01-01.json", [])
        (tmp_path / "data/staging/generation").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_generation("2022-01-01")

        assert not (tmp_path / "data/staging/generation/2022-01-01.parquet").exists()


class TestStageRegionalIntensity:
    def test_stage_regional_intensity_writes_flattened_parquet(
        self, tmp_path, monkeypatch
    ):
        _write_json(
            tmp_path,
            "data/raw/regional_intensity/2022-01-01.json",
            SAMPLE_REGIONAL_INTENSITY,
        )
        (tmp_path / "data/staging/regional_intensity").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_regional_intensity("2022-01-01")

        result = _read_parquet(
            tmp_path, "data/staging/regional_intensity/2022-01-01.parquet"
        )
        assert len(result) == 2
        assert set(result["fuel"]) == {"wind", "hydro"}
        assert set(result["percentage"]) == {80.0, 15.0}
        assert set(result["regionid"]) == {1}
        assert set(result["shortname"]) == {"North Scotland"}
        assert set(result["intensity.forecast"]) == {42}
        assert set(result["intensity.index"]) == {"very low"}

    def test_stage_regional_intensity_skips_empty_input(self, tmp_path, monkeypatch):
        _write_json(tmp_path, "data/raw/regional_intensity/2022-01-01.json", [])
        (tmp_path / "data/staging/regional_intensity").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        staging.stage_regional_intensity("2022-01-01")

        assert not (
            tmp_path / "data/staging/regional_intensity/2022-01-01.parquet"
        ).exists()


class TestMain:
    @patch("src.storage.staging.stage_regional_intensity")
    @patch("src.storage.staging.stage_generation")
    @patch("src.storage.staging.stage_national_intensity")
    def test_main_creates_dirs_and_stages_each_date(
        self, mock_national, mock_generation, mock_regional, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)

        staging.main("2022-01-01", "2022-01-02")

        assert (tmp_path / "data/staging/national_intensity").exists()
        assert (tmp_path / "data/staging/generation").exists()
        assert (tmp_path / "data/staging/regional_intensity").exists()

        assert mock_national.call_count == 2
        assert mock_generation.call_count == 2
        assert mock_regional.call_count == 2

        mock_national.assert_any_call("2022-01-01")
        mock_national.assert_any_call("2022-01-02")
        mock_generation.assert_any_call("2022-01-01")
        mock_generation.assert_any_call("2022-01-02")
        mock_regional.assert_any_call("2022-01-01")
        mock_regional.assert_any_call("2022-01-02")
