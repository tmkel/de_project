"""Tests for validation/validate.py."""

import json
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src import config
from src.validation import validate
from src.validation.validate import (
    GenerationMixRecord,
    IntensityRecord,
    RegionalIntensityRecord,
    validate_generation_mix_response,
    validate_intensity_response,
    validate_regional_response,
)


VALID_INTENSITY_RECORD = {
    "from": "2022-01-01T00:00Z",
    "to": "2022-01-01T00:30Z",
    "intensity": {"forecast": 195, "actual": 193, "index": "moderate"},
}

VALID_GENERATION_RECORD = {
    "from": "2022-01-01T00:00Z",
    "to": "2022-01-01T00:30Z",
    "generationmix": [
        {"fuel": "gas", "perc": 35.2},
        {"fuel": "wind", "perc": 25.0},
        {"fuel": "nuclear", "perc": 18.5},
    ],
}

VALID_REGIONAL_RECORD = {
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
                {"fuel": "gas", "perc": 5.0},
            ],
        }
    ],
}


class TestIntensityRecord:
    def test_valid_record(self):
        record = IntensityRecord.model_validate(VALID_INTENSITY_RECORD)

        assert record.from_ == "2022-01-01T00:00Z"
        assert record.to == "2022-01-01T00:30Z"
        assert record.intensity.forecast == 195
        assert record.intensity.actual == 193
        assert record.intensity.index == "moderate"

    def test_null_actual(self):
        data = {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "intensity": {"forecast": 195, "actual": None, "index": "moderate"},
        }

        record = IntensityRecord.model_validate(data)

        assert record.intensity.actual is None

    def test_invalid_index(self):
        data = {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "intensity": {"forecast": 195, "actual": 193, "index": "extreme"},
        }

        with pytest.raises(ValidationError):
            IntensityRecord.model_validate(data)

    def test_missing_intensity_key(self):
        data = {"from": "2022-01-01T00:00Z", "to": "2022-01-01T00:30Z"}

        with pytest.raises(ValidationError):
            IntensityRecord.model_validate(data)


class TestGenerationMixRecord:
    def test_valid_record(self):
        record = GenerationMixRecord.model_validate(VALID_GENERATION_RECORD)

        assert record.from_ == "2022-01-01T00:00Z"
        assert len(record.generationmix) == 3
        assert record.generationmix[0].fuel == "gas"

    def test_fuel_percentage_out_of_range(self):
        data = {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "generationmix": [{"fuel": "wind", "perc": 150.0}],
        }

        with pytest.raises(ValidationError):
            GenerationMixRecord.model_validate(data)

    def test_negative_percentage(self):
        data = {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "generationmix": [{"fuel": "wind", "perc": -5.0}],
        }

        with pytest.raises(ValidationError):
            GenerationMixRecord.model_validate(data)


class TestRegionalIntensityRecord:
    def test_valid_record(self):
        record = RegionalIntensityRecord.model_validate(VALID_REGIONAL_RECORD)

        assert record.from_ == "2022-01-01T00:00Z"
        assert len(record.regions) == 1
        assert record.regions[0].shortname == "North Scotland"
        assert record.regions[0].intensity.index == "very low"
        assert len(record.regions[0].generationmix) == 3


class TestValidationHelpers:
    def test_validate_intensity_good_data(self):
        results = validate_intensity_response([VALID_INTENSITY_RECORD])

        assert len(results) == 1
        assert results[0].intensity.forecast == 195

    def test_validate_intensity_mixed_data(self):
        bad_record = {"from": "2022-01-01T00:00Z", "to": "2022-01-01T00:30Z"}

        results = validate_intensity_response([VALID_INTENSITY_RECORD, bad_record])

        assert len(results) == 1

    def test_validate_intensity_empty(self):
        results = validate_intensity_response([])

        assert results == []

    def test_validate_generation_good_data(self):
        results = validate_generation_mix_response([VALID_GENERATION_RECORD])

        assert len(results) == 1
        assert results[0].generationmix[1].fuel == "wind"

    def test_validate_generation_mixed_data(self):
        bad_record = {
            "from": "2022-01-01T00:00Z",
            "to": "2022-01-01T00:30Z",
            "generationmix": [{"fuel": "wind", "perc": 150.0}],
        }

        results = validate_generation_mix_response(
            [VALID_GENERATION_RECORD, bad_record]
        )

        assert len(results) == 1

    def test_validate_regional_good_data(self):
        results = validate_regional_response([VALID_REGIONAL_RECORD])

        assert len(results) == 1
        assert results[0].regions[0].regionid == 1


class TestValidateDate:
    def test_writes_validated_zone_and_preserves_raw(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        raw_records = [
            VALID_INTENSITY_RECORD,
            {
                "from": "2022-01-01T00:30Z",
                "to": "2022-01-01T01:00Z",
                "intensity": {"forecast": 120, "actual": 110, "index": "extreme"},
            },
        ]
        raw_file = config.raw_path(config.NATIONAL_INTENSITY, "2022-01-01")
        raw_file.parent.mkdir(parents=True)
        raw_file.write_text(json.dumps(raw_records))

        validate.validate_date("2022-01-01")

        # Sanitized output lands in the validated zone, invalid record dropped.
        validated_file = config.validated_path(config.NATIONAL_INTENSITY, "2022-01-01")
        saved_records = json.loads(validated_file.read_text())
        assert len(saved_records) == 1
        assert saved_records[0]["from"] == "2022-01-01T00:00Z"
        assert saved_records[0]["intensity"]["index"] == "moderate"

        # Raw zone is left untouched.
        assert json.loads(raw_file.read_text()) == raw_records

    def test_skips_dataset_with_no_raw_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        validate.validate_date("2022-01-01")

        for dataset in config.DATASETS:
            assert not config.validated_path(dataset, "2022-01-01").exists()

    @patch("src.validation.validate.validate_date")
    def test_main_validates_each_day(self, mock_validate_date):
        validate.main("2022-01-01", "2022-01-02")

        assert mock_validate_date.call_count == 2
        mock_validate_date.assert_any_call("2022-01-01")
        mock_validate_date.assert_any_call("2022-01-02")
