import json
from unittest.mock import patch

import pandas as pd

from src import config
from src.staging import staging


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


def _write_validated(dataset, date, payload):
    """Seed the validated zone (staging's input) for a dataset/day."""
    file_path = config.validated_path(dataset, date)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload))
    return file_path


def _make_staging_dir(dataset):
    staging_dir = config.STAGING_DIR / dataset
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir


class TestLoadJson:
    def test_load_json_reads_file_contents(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        file_path = _write_validated(
            config.NATIONAL_INTENSITY, "2022-01-01", SAMPLE_NATIONAL_INTENSITY
        )

        result = staging.load_json(file_path)

        assert result == SAMPLE_NATIONAL_INTENSITY


class TestStageNationalIntensity:
    def test_stage_national_intensity_writes_parquet(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write_validated(
            config.NATIONAL_INTENSITY, "2022-01-01", SAMPLE_NATIONAL_INTENSITY
        )
        _make_staging_dir(config.NATIONAL_INTENSITY)

        staging.stage_national_intensity("2022-01-01")

        result = pd.read_parquet(
            config.staging_path(config.NATIONAL_INTENSITY, "2022-01-01")
        )
        assert len(result) == 1
        assert result.loc[0, "from"] == "2022-01-01T00:00Z"
        assert result.loc[0, "intensity.forecast"] == 195
        assert result.loc[0, "intensity.actual"] == 193
        assert result.loc[0, "intensity.index"] == "moderate"

    def test_stage_national_intensity_skips_empty_input(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write_validated(config.NATIONAL_INTENSITY, "2022-01-01", [])
        _make_staging_dir(config.NATIONAL_INTENSITY)

        staging.stage_national_intensity("2022-01-01")

        assert not config.staging_path(
            config.NATIONAL_INTENSITY, "2022-01-01"
        ).exists()


class TestStageGeneration:
    def test_stage_generation_writes_flattened_parquet(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write_validated(
            config.NATIONAL_GENERATION_MIX, "2022-01-01", SAMPLE_GENERATION
        )
        _make_staging_dir(config.NATIONAL_GENERATION_MIX)

        staging.stage_national_generation_mix("2022-01-01")

        result = pd.read_parquet(
            config.staging_path(config.NATIONAL_GENERATION_MIX, "2022-01-01")
        )
        assert len(result) == 2
        assert set(result["fuel"]) == {"wind", "gas"}
        assert set(result["perc"]) == {25.0, 35.2}
        assert set(result["from"]) == {"2022-01-01T00:00Z"}
        assert set(result["to"]) == {"2022-01-01T00:30Z"}

    def test_stage_generation_skips_empty_input(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write_validated(config.NATIONAL_GENERATION_MIX, "2022-01-01", [])
        _make_staging_dir(config.NATIONAL_GENERATION_MIX)

        staging.stage_national_generation_mix("2022-01-01")

        assert not config.staging_path(
            config.NATIONAL_GENERATION_MIX, "2022-01-01"
        ).exists()


class TestStageRegionalIntensity:
    def test_stage_regional_intensity_writes_flattened_parquet(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        _write_validated(
            config.REGIONAL_INTENSITY_GENERATION_MIX,
            "2022-01-01",
            SAMPLE_REGIONAL_INTENSITY,
        )
        _make_staging_dir(config.REGIONAL_INTENSITY_GENERATION_MIX)

        staging.stage_regional_intensity_generation_mix("2022-01-01")

        result = pd.read_parquet(
            config.staging_path(config.REGIONAL_INTENSITY_GENERATION_MIX, "2022-01-01")
        )
        assert len(result) == 2
        assert set(result["fuel"]) == {"wind", "hydro"}
        assert set(result["percentage"]) == {80.0, 15.0}
        assert set(result["regionid"]) == {1}
        assert set(result["shortname"]) == {"North Scotland"}
        assert set(result["intensity.forecast"]) == {42}
        assert set(result["intensity.index"]) == {"very low"}

    def test_stage_regional_intensity_skips_empty_input(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write_validated(config.REGIONAL_INTENSITY_GENERATION_MIX, "2022-01-01", [])
        _make_staging_dir(config.REGIONAL_INTENSITY_GENERATION_MIX)

        staging.stage_regional_intensity_generation_mix("2022-01-01")

        assert not config.staging_path(
            config.REGIONAL_INTENSITY_GENERATION_MIX, "2022-01-01"
        ).exists()


class TestMain:
    @patch("src.staging.staging.stage_regional_intensity_generation_mix")
    @patch("src.staging.staging.stage_national_generation_mix")
    @patch("src.staging.staging.stage_national_intensity")
    def test_main_creates_dirs_and_stages_each_date(
        self, mock_national, mock_generation, mock_regional, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)

        staging.main("2022-01-01", "2022-01-02")

        for dataset in config.DATASETS:
            assert (config.STAGING_DIR / dataset).exists()

        assert mock_national.call_count == 2
        assert mock_generation.call_count == 2
        assert mock_regional.call_count == 2

        mock_national.assert_any_call("2022-01-01")
        mock_national.assert_any_call("2022-01-02")
        mock_generation.assert_any_call("2022-01-01")
        mock_generation.assert_any_call("2022-01-02")
        mock_regional.assert_any_call("2022-01-01")
        mock_regional.assert_any_call("2022-01-02")
