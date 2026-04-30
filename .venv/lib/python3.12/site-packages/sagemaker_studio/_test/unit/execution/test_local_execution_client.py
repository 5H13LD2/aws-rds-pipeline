import json
import unittest
from typing import Any, Dict
from unittest.mock import patch

from sagemaker_studio.data_models import HttpMethod
from sagemaker_studio.execution.local_execution_client import LocalExecutionClient
from sagemaker_studio.execution.utils import LocalExecutionUtils
from sagemaker_studio.models.execution import ConflictError, ExecutionConfig

get_execution_response = {
    "status": "COMPLETED",
    "downloaded": True,
    "start_time": "1970-01-01T00:00:00.000000Z",
    "end_time": "1971-01-01T00:00:00.000000Z",
    "error_details": None,
    "job_files": [],
}
list_executions_response = {
    "jobs": [
        {
            "job_id": "bogus_execution_id",
            "name": "bogus-execution-name",
            "status": "COMPLETED",
            "start_time": "1970-01-01T00:00:00.000000Z",
            "end_time": "1971-01-01T00:00:00.000000Z",
        },
        {
            "job_id": "other_execution_id",
            "name": "other-execution-name",
            "status": "IN_PROGRESS",
            "start_time": "1970-01-01T00:00:00.000000Z",
            "end_time": "1971-01-01T00:00:00.000000Z",
        },
    ],
    "next_token": None,
}
start_execution_response = {"job_id": "bogus_execution_id"}
stop_execution_response: Dict[Any, Any] = {}


class TestLocalExecutionClient(unittest.TestCase):

    class MockLocalExecutionUtils:
        @staticmethod
        def mock_fetch_data(url: str, params: dict = {}) -> dict:
            print(f"url {url} {params.get('method')}")
            if (
                url == f"{LocalExecutionUtils.LOCAL_JOBS_SCHEDULER_URL}/bogus_execution_id"
                and not params
            ):  # get_execution
                return get_execution_response
            elif (
                url
                == f"{LocalExecutionUtils.LOCAL_EXECUTION_HOST}/jupyterlab/default/scheduler/jobs?sort_by=desc%28start_time%29"
            ):  # list_executions
                return list_executions_response
            elif (
                url == f"{LocalExecutionUtils.LOCAL_JOBS_SCHEDULER_URL}/bogus_execution_id"
                and params.get("method") == HttpMethod.PATCH
            ):  # stop_execution
                return stop_execution_response
            elif (
                url == LocalExecutionUtils.LOCAL_JOBS_SCHEDULER_URL
                and params.get("method") == HttpMethod.POST
            ):  # start_execution
                return start_execution_response
            else:
                return {}

        @staticmethod
        def mock_get_bytes_free(folder: str):
            return 5000

        @staticmethod
        def mock_get_bytes_needed(folder: str):
            return 1000

        @staticmethod
        def mock_cpu_count():
            return 10000

    def setUp(self):
        patcher = patch(
            "sagemaker_studio.execution.local_execution_client.LocalExecutionClient._LocalExecutionClient__set_cookies"
        )
        self.mock_set_cookies = patcher.start()
        self.mock_set_cookies.return_value = "bogus_xsrf_token"
        self.addCleanup(patcher.stop)
        self.local_client = LocalExecutionClient(ExecutionConfig())

    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.fetch_data")
    def test_get_execution(self, mock_fetch_data_func):
        mock_fetch_data_func.side_effect = self.MockLocalExecutionUtils.mock_fetch_data
        assert self.local_client.get_execution("bogus_execution_id") == {
            "execution_id": "bogus_execution_id",
            "status": get_execution_response["status"],
            "start_time": get_execution_response["start_time"],
            "end_time": get_execution_response["end_time"],
            "files": [],
        }

    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.fetch_data")
    def test_list_executions(self, mock_fetch_data_func):
        mock_fetch_data_func.side_effect = self.MockLocalExecutionUtils.mock_fetch_data
        assert self.local_client.list_executions() == {
            "executions": [
                {
                    "id": "bogus_execution_id",
                    "name": "bogus-execution-name",
                    "status": "COMPLETED",
                    "start_time": "1970-01-01T00:00:00.000000Z",
                    "end_time": "1971-01-01T00:00:00.000000Z",
                },
                {
                    "id": "other_execution_id",
                    "name": "other-execution-name",
                    "status": "IN_PROGRESS",
                    "start_time": "1970-01-01T00:00:00.000000Z",
                    "end_time": "1971-01-01T00:00:00.000000Z",
                },
            ],
            "next_token": None,
        }

    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.get_bytes_needed")
    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.get_bytes_free")
    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.fetch_data")
    # Mocked in the unit tests because we do not want to fail the test based on the
    # load on the test runner
    @patch("psutil.getloadavg", lambda: [40])
    @patch("psutil.cpu_count", lambda: 45)
    def test_start_execution(
        self,
        mock_fetch_data_func,
        mock_get_bytes_free_func,
        mock_get_bytes_needed_func,
    ):
        mock_fetch_data_func.side_effect = self.MockLocalExecutionUtils.mock_fetch_data
        mock_get_bytes_free_func.side_effect = self.MockLocalExecutionUtils.mock_get_bytes_free
        mock_get_bytes_needed_func.side_effect = self.MockLocalExecutionUtils.mock_get_bytes_needed

        input_config = {
            "notebook_config": {
                "input_path": "bogus_input_path",
                "input_parameters": {"param1": "bogus_value"},
            }
        }

        assert json.dumps(
            self.local_client.start_execution(
                execution_name="bogus-execution-name",
                input_config=input_config,
            )
        ) == json.dumps(
            {
                "execution_id": start_execution_response["job_id"],
                "execution_name": "bogus-execution-name",
                "input_config": {
                    "notebook_config": {
                        "input_path": "bogus_input_path",
                        "input_parameters": {"param1": "bogus_value"},
                    }
                },
                "output_config": {"notebook_config": {"output_formats": ["NOTEBOOK"]}},
            }
        )

    @patch("sagemaker_studio.execution.utils.LocalExecutionUtils.fetch_data")
    def test_stop_execution(self, mock_fetch_data_func):
        mock_fetch_data_func.side_effect = self.MockLocalExecutionUtils.mock_fetch_data
        with self.assertRaises(ConflictError):
            self.local_client.stop_execution("bogus_execution_id")
