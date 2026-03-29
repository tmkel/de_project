import json
from unittest.mock import MagicMock, patch

import requests

from src.ingestion import api_client


def _mock_response(json_data, status_code=200):
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_data
    mock_resp.status_code = status_code
    mock_resp.raise_for_status.return_value = None
    return mock_resp


class TestFetchData:
    @patch("src.ingestion.api_client.requests.get")
    def test_returns_data_from_response_payload(self, mock_get):
        mock_get.return_value = _mock_response(
            {"data": [{"from": "2022-01-01T00:00Z", "value": 1}]}
        )

        result = api_client._fetch_data("/intensity/date/2022-01-01", "2022-01-01")

        assert result == [{"from": "2022-01-01T00:00Z", "value": 1}]
        mock_get.assert_called_once_with(
            "https://api.carbonintensity.org.uk/intensity/date/2022-01-01",
            headers=api_client.headers,
            timeout=30,
        )

    @patch("src.ingestion.api_client.requests.get")
    def test_returns_empty_list_on_timeout(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout

        result = api_client._fetch_data("/intensity/date/2022-01-01", "2022-01-01")

        assert result == []

    @patch("src.ingestion.api_client.requests.get")
    def test_returns_empty_list_on_http_error(self, mock_get):
        mock_response = _mock_response({"data": []}, status_code=500)
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error"
        )
        mock_get.return_value = mock_response

        result = api_client._fetch_data("/intensity/date/2022-01-01", "2022-01-01")

        assert result == []

    @patch("src.ingestion.api_client.requests.get")
    def test_returns_empty_list_on_unexpected_error(self, mock_get):
        mock_get.side_effect = Exception("boom")

        result = api_client._fetch_data("/intensity/date/2022-01-01", "2022-01-01")

        assert result == []


class TestGetters:
    @patch("src.ingestion.api_client.requests.get")
    def test_get_carbon_intensity_national_returns_payload_data(self, mock_get):
        mock_get.return_value = _mock_response(
            {
                "data": [
                    {
                        "from": "2022-01-01T00:00Z",
                        "to": "2022-01-01T00:30Z",
                        "intensity": {
                            "forecast": 74,
                            "actual": 74,
                            "index": "low",
                        },
                    }
                ]
            }
        )

        result = api_client.get_carbon_intensity_national("2022-01-01")

        assert len(result) == 1
        assert result[0]["intensity"]["forecast"] == 74

    @patch("src.ingestion.api_client._fetch_data")
    def test_get_generation_mix_national_uses_next_day_endpoint_and_filters(self, mock_fetch):
        mock_fetch.return_value = [
            {"from": "2022-01-01T00:00Z", "generationmix": [{"fuel": "wind"}]},
            {"from": "2022-01-01T23:30Z", "generationmix": [{"fuel": "gas"}]},
            {"from": "2022-01-02T00:00Z", "generationmix": [{"fuel": "solar"}]},
        ]

        result = api_client.get_generation_mix_national("2022-01-01")

        assert len(result) == 2
        assert all(record["from"].startswith("2022-01-01") for record in result)
        mock_fetch.assert_called_once_with(
            endpoint="/generation/2022-01-02/pt24h",
            context="2022-01-01",
        )

    @patch("src.ingestion.api_client._fetch_data")
    def test_get_intensity_gm_regional_uses_next_day_endpoint_and_filters(
        self, mock_fetch
    ):
        mock_fetch.return_value = [
            {"from": "2022-01-01T00:00Z", "regions": [{"regionid": 1}]},
            {"from": "2022-01-02T00:00Z", "regions": [{"regionid": 2}]},
        ]

        result = api_client.get_intensity_gm_regional("2022-01-01")

        assert result == [{"from": "2022-01-01T00:00Z", "regions": [{"regionid": 1}]}]
        mock_fetch.assert_called_once_with(
            endpoint="/regional/intensity/2022-01-02/pt24h",
            context="regional data on 2022-01-01",
        )


class TestFetchDailyDatasets:
    @patch("src.ingestion.api_client.get_intensity_gm_regional")
    @patch("src.ingestion.api_client.get_generation_mix_national")
    @patch("src.ingestion.api_client.get_carbon_intensity_national")
    def test_combines_all_dataset_results(
        self, mock_intensity, mock_generation, mock_regional
    ):
        mock_intensity.return_value = [{"kind": "intensity"}]
        mock_generation.return_value = [{"kind": "generation"}]
        mock_regional.return_value = [{"kind": "regional"}]

        result = api_client.fetch_daily_datasets("2022-01-01")

        assert result == {
            "national_intensity": [{"kind": "intensity"}],
            "generation": [{"kind": "generation"}],
            "regional_intensity": [{"kind": "regional"}],
        }


class TestSaveDailyDatasets:
    def test_writes_each_dataset_to_its_target_file(self, tmp_path):
        output_dirs = {
            "national_intensity": str(tmp_path / "national_intensity"),
            "generation": str(tmp_path / "generation"),
            "regional_intensity": str(tmp_path / "regional_intensity"),
        }
        datasets = {
            "national_intensity": [{"value": 1}],
            "generation": [{"value": 2}],
            "regional_intensity": [{"value": 3}],
        }

        api_client.save_daily_datasets("2022-01-01", datasets, output_dirs=output_dirs)

        assert json.loads(
            (tmp_path / "national_intensity" / "2022-01-01.json").read_text()
        ) == [{"value": 1}]
        assert json.loads(
            (tmp_path / "generation" / "2022-01-01.json").read_text()
        ) == [{"value": 2}]
        assert json.loads(
            (tmp_path / "regional_intensity" / "2022-01-01.json").read_text()
        ) == [{"value": 3}]

    def test_writes_empty_list_for_missing_dataset_key(self, tmp_path):
        output_dirs = {"national_intensity": str(tmp_path / "national_intensity")}

        api_client.save_daily_datasets("2022-01-01", {}, output_dirs=output_dirs)

        assert json.loads(
            (tmp_path / "national_intensity" / "2022-01-01.json").read_text()
        ) == []


class TestMain:
    @patch("src.ingestion.api_client.time.sleep")
    @patch("src.ingestion.api_client.save_daily_datasets")
    @patch("src.ingestion.api_client.fetch_daily_datasets")
    def test_main_fetches_and_saves_each_day(self, mock_fetch_daily, mock_save, mock_sleep):
        mock_fetch_daily.side_effect = [
            {"national_intensity": [1], "generation": [2], "regional_intensity": [3]},
            {"national_intensity": [4], "generation": [5], "regional_intensity": [6]},
        ]

        api_client.main("2022-01-01", "2022-01-02")

        assert mock_fetch_daily.call_count == 2
        mock_fetch_daily.assert_any_call("2022-01-01")
        mock_fetch_daily.assert_any_call("2022-01-02")

        assert mock_save.call_count == 2
        mock_save.assert_any_call(
            "2022-01-01",
            {"national_intensity": [1], "generation": [2], "regional_intensity": [3]},
        )
        mock_save.assert_any_call(
            "2022-01-02",
            {"national_intensity": [4], "generation": [5], "regional_intensity": [6]},
        )

        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(0.5)
