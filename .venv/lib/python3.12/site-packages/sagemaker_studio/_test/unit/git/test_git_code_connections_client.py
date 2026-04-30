import unittest
from unittest.mock import patch

from sagemaker_studio.git import GitCodeConnectionsClient


class TestGitCodeConnectionsClient(unittest.TestCase):
    def setUp(self):
        self.git_code_connections_client = GitCodeConnectionsClient(
            connection_arn="arn:aws:codeconnections:us-west-2:637423344544:connection/3a8f223d-a655-4c02-bc56-6f767a948b21"
        )

    @patch.object(GitCodeConnectionsClient, "_parse_arn")
    def test_get_clone_url(self, mock_parse_arn):
        mock_parse_arn.return_value = {
            "region": "us-west-2",
            "account_id": "637423344544",
            "resource": "connection/3a8f223d-a655-4c02-bc56-6f767a948b21",
        }
        result = self.git_code_connections_client.get_clone_url("AWSMD/crispy-winner")
        expected_url = "https://codeconnections.us-west-2.amazonaws.com/git-http/637423344544/us-west-2/3a8f223d-a655-4c02-bc56-6f767a948b21/AWSMD/crispy-winner.git"
        self.assertEqual(result, {"cloneUrl": expected_url})

    @patch.object(GitCodeConnectionsClient, "_parse_arn")
    def test_get_clone_url_invalid_arn(self, mock_parse_arn):
        mock_parse_arn.return_value = {
            "region": "us-west-2",
            "account_id": "637423344544",
            "resource": "invalid-resource-format",
        }
        with self.assertRaises(ValueError) as context:
            self.git_code_connections_client.get_clone_url("my-repository-id")

        self.assertEqual(
            str(context.exception),
            "Invalid resource format in ARN: arn:aws:codeconnections:us-west-2:637423344544:connection/3a8f223d-a655-4c02-bc56-6f767a948b21",
        )
