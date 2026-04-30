import unittest
from unittest.mock import Mock, patch

from sagemaker_studio.git import GitCodeCommitClient


class TestGitCodeCommitClient(unittest.TestCase):
    def setUp(self):
        self.mock_datazone_api = Mock()
        self.mock_project_api = Mock()
        self.git_code_commit_client = GitCodeCommitClient(
            datazone_api=self.mock_datazone_api,
            project_api=self.mock_project_api,
            domain_identifier="dzd_bx2a2afyvn2hlc",
            project_identifier="449et7665a6p3k",
        )

    @patch.object(GitCodeCommitClient, "_get_client")
    def test_get_clone_url(self, mock_get_client):
        mock_code_commit_client = Mock()
        mock_get_client.return_value = mock_code_commit_client
        mock_code_commit_client.get_repository.return_value = {
            "repositoryMetadata": {
                "cloneUrlHttp": "https://git-codecommit.us-west-2.amazonaws.com/v1/repos/sagemaker-studio-61p1t7s1b4n40g-dev"
            }
        }
        result = self.git_code_commit_client.get_clone_url("sagemaker-studio-61p1t7s1b4n40g-dev")
        self.assertEqual(
            result,
            {
                "cloneUrl": "https://git-codecommit.us-west-2.amazonaws.com/v1/repos/sagemaker-studio-61p1t7s1b4n40g-dev"
            },
        )
        mock_code_commit_client.get_repository.assert_called_once_with(
            repositoryName="sagemaker-studio-61p1t7s1b4n40g-dev"
        )
