import json
from unittest.mock import patch

from run_pipeline import build_parser, run_pipeline, validate_raw_data


def test_validate_raw_data_filters_invalid_records(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    raw_dir = tmp_path / "data/raw/national_intensity"
    raw_dir.mkdir(parents=True)
    file_path = raw_dir / "2022-01-01.json"
    file_path.write_text(
        json.dumps(
            [
                {
                    "from": "2022-01-01T00:00Z",
                    "to": "2022-01-01T00:30Z",
                    "intensity": {
                        "forecast": 100,
                        "actual": 95,
                        "index": "low",
                    },
                },
                {
                    "from": "2022-01-01T00:30Z",
                    "to": "2022-01-01T01:00Z",
                    "intensity": {
                        "forecast": 120,
                        "actual": 110,
                        "index": "extreme",
                    },
                },
            ]
        )
    )

    validate_raw_data("2022-01-01")

    saved_records = json.loads(file_path.read_text())
    assert len(saved_records) == 1
    assert saved_records[0]["from"] == "2022-01-01T00:00Z"
    assert saved_records[0]["intensity"]["index"] == "low"


class TestRunPipeline:
    @patch("run_pipeline.loader.main")
    @patch("run_pipeline.staging.main")
    @patch("run_pipeline.validate_raw_data")
    @patch("run_pipeline.api_client.main")
    def test_runs_all_steps_in_order(
        self, mock_ingest, mock_validate, mock_stage, mock_load
    ):
        run_pipeline("2022-01-01", "2022-01-02")

        mock_ingest.assert_called_once_with("2022-01-01", "2022-01-02")
        assert mock_validate.call_args_list == [
            (("2022-01-01",), {}),
            (("2022-01-02",), {}),
        ]
        mock_stage.assert_called_once_with("2022-01-01", "2022-01-02")
        mock_load.assert_called_once_with("2022-01-01", "2022-01-02")

    @patch("run_pipeline.loader.main")
    @patch("run_pipeline.staging.main")
    @patch("run_pipeline.validate_raw_data")
    @patch("run_pipeline.api_client.main")
    def test_skip_ingest_reuses_existing_raw_files(
        self, mock_ingest, mock_validate, mock_stage, mock_load
    ):
        run_pipeline("2022-01-01", "2022-01-01", skip_ingest=True)

        mock_ingest.assert_not_called()
        mock_validate.assert_called_once_with("2022-01-01")
        mock_stage.assert_called_once_with("2022-01-01", "2022-01-01")
        mock_load.assert_called_once_with("2022-01-01", "2022-01-01")

    @patch("run_pipeline.loader.main")
    @patch("run_pipeline.staging.main")
    @patch("run_pipeline.validate_raw_data")
    @patch("run_pipeline.api_client.main")
    def test_skip_load_stops_after_staging(
        self, mock_ingest, mock_validate, mock_stage, mock_load
    ):
        run_pipeline("2022-01-01", "2022-01-01", skip_load=True)

        mock_ingest.assert_called_once_with("2022-01-01", "2022-01-01")
        mock_validate.assert_called_once_with("2022-01-01")
        mock_stage.assert_called_once_with("2022-01-01", "2022-01-01")
        mock_load.assert_not_called()


def test_build_parser_parses_flags():
    parser = build_parser()

    args = parser.parse_args(
        [
            "--from-date",
            "2022-01-01",
            "--to-date",
            "2022-01-02",
            "--skip-ingest",
            "--skip-load",
        ]
    )

    assert args.from_date == "2022-01-01"
    assert args.to_date == "2022-01-02"
    assert args.skip_ingest is True
    assert args.skip_load is True
