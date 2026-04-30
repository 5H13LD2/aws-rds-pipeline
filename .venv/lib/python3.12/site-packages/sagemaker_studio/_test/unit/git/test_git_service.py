import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from sagemaker_studio.git import GitService


class TestGitService(unittest.TestCase):
    def setUp(self):
        self.mock_datazone_api = Mock()
        self.mock_project_api = Mock()
        self.git_service = GitService(
            datazone_api=self.mock_datazone_api, project_api=self.mock_project_api
        )

    def test_get_clone_url_validation(self):
        invalid_domain_id = "invalid_domain_id"
        invalid_project_id = "invalid_project_id"
        with self.assertRaises(ValueError) as context:
            self.git_service.get_clone_url(
                domain_identifier=invalid_domain_id, project_identifier=invalid_project_id
            )
        self.assertIn("Invalid value for `domain_identifier`", str(context.exception))

    @patch.object(GitService, "_resolve_client")
    @patch.object(GitService, "_get_repository_id")
    def test_get_clone_url_valid(self, mock_get_repository_id, mock_resolve_client):
        mock_git_client = Mock()
        mock_git_client.get_clone_url.return_value = "https://git-url"
        mock_resolve_client.return_value = mock_git_client
        mock_get_repository_id.return_value = "repository-id"
        clone_url = self.git_service.get_clone_url("dzd_bx2a2afyvn2hlc", "449et7665a6p3k")
        mock_resolve_client.assert_called_once()
        mock_get_repository_id.assert_called_once()
        self.assertEqual(clone_url, "https://git-url")

    @patch.object(GitService, "_get_connection_arn", return_value=None)
    def test_resolve_client_with_missing_identifiers(self, mock_get_connection_arn):
        self.git_service.domain_identifier = None
        self.git_service.project_identifier = None
        with self.assertRaises(ValueError) as context:
            self.git_service._resolve_client()
        self.assertEqual(str(context.exception), "Domain and project identifiers cannot be None")

    @patch.object(GitService, "_get_connection_arn", return_value=None)
    def test_resolve_client_to_return_git_code_commit_client(self, mock_get_connection_arn):
        self.git_service.domain_identifier = "dzd_bx2a2afyvn2hlc"
        self.git_service.project_identifier = "449et7665a6p3k"
        with patch("sagemaker_studio.git.GitCodeCommitClient") as mock_git_code_commit_client:
            client = self.git_service._resolve_client()
            mock_git_code_commit_client.assert_called_once_with(
                self.git_service.project_api,
                self.git_service.datazone_api,
                "dzd_bx2a2afyvn2hlc",
                "449et7665a6p3k",
            )
            self.assertEqual(client, mock_git_code_commit_client.return_value)

    @patch.object(
        GitService,
        "_get_connection_arn",
        return_value='"arn:aws:codeconnections:region:account:connection/uuid"',
    )
    def test_resolve_client_to_return_git_code_connections_client(self, mock_get_connection_arn):
        with patch(
            "sagemaker_studio.git.GitCodeConnectionsClient"
        ) as mock_git_code_connections_client:
            client = self.git_service._resolve_client()
            mock_git_code_connections_client.assert_called_once_with(
                '"arn:aws:codeconnections:region:account:connection/uuid"'
            )
            self.assertEqual(client, mock_git_code_connections_client.return_value)

    @patch.object(GitService, "_get_provisioned_resources")
    def test_get_connection_arn_found(self, mock_get_provisioned_resources):
        mock_get_provisioned_resources.return_value = [
            {"name": "otherResource", "value": "some-value"},
            {
                "name": "gitConnectionArn",
                "value": "arn:aws:codecommit:us-west-2:123456789012:connection/uuid",
            },
        ]
        connection_arn = self.git_service._get_connection_arn()
        self.assertEqual(
            connection_arn, "arn:aws:codecommit:us-west-2:123456789012:connection/uuid"
        )

    @patch.object(GitService, "_get_provisioned_resources")
    def test_get_connection_arn_not_found(self, mock_get_provisioned_resources):
        mock_get_provisioned_resources.return_value = [
            {"name": "otherResource", "value": "some-value"}
        ]
        connection_arn = self.git_service._get_connection_arn()
        self.assertIsNone(connection_arn)

    @patch.object(GitService, "_get_provisioned_resources")
    def test_get_connection_arn_empty_resources(self, mock_get_provisioned_resources):
        mock_get_provisioned_resources.return_value = []
        connection_arn = self.git_service._get_connection_arn()
        self.assertIsNone(connection_arn)

    @patch.object(GitService, "_get_provisioned_resources")
    def test_get_repository_id_code_repository_name(self, mock_get_provisioned_resources):
        mock_get_provisioned_resources.return_value = [
            {"name": "otherResource", "value": "some-value"},
            {"name": "codeRepositoryName", "value": "repository-id-123"},
        ]
        repository_id = self.git_service._get_repository_id()
        self.assertEqual(repository_id, "repository-id-123")

    @patch.object(GitService, "_get_provisioned_resources")
    def test_get_repository_id_git_full_repository_id(self, mock_get_provisioned_resources):
        mock_get_provisioned_resources.return_value = [
            {"name": "gitFullRepositoryId", "value": "full-repo-id-456"},
            {"name": "otherResource", "value": "some-value"},
        ]
        repository_id = self.git_service._get_repository_id()
        self.assertEqual(repository_id, "full-repo-id-456")

    def test_provisioned_resources_missing_identifiers(self):
        self.git_service.domain_identifier = None
        self.git_service.project_identifier = "valid-project"
        with self.assertRaises(ValueError) as context:
            self.git_service._get_provisioned_resources()
        self.assertEqual(str(context.exception), "Domain and project identifiers cannot be None")
        self.git_service.domain_identifier = "valid-domain"
        self.git_service.project_identifier = None
        with self.assertRaises(ValueError) as context:
            self.git_service._get_provisioned_resources()
        self.assertEqual(str(context.exception), "Domain and project identifiers cannot be None")

    def test_project_default_environment_none(self):
        self.git_service.domain_identifier = "valid-domain"
        self.git_service.project_identifier = "valid-project"
        self.mock_project_api.get_project_default_environment.return_value = None
        with self.assertRaises(ValueError) as context:
            self.git_service._get_provisioned_resources()
        self.assertEqual(str(context.exception), "Project Default Environment Does Not Exist")

    def test_provisioned_resources_valid(self):
        self.git_service.domain_identifier = "valid-domain"
        self.git_service.project_identifier = "valid-project"
        self.mock_project_api.get_project_default_environment.return_value = {
            "provisionedResources": [
                {"name": "resource1", "value": "value1"},
                {"name": "resource2", "value": "value2"},
            ]
        }
        provisioned_resources = self.git_service._get_provisioned_resources()
        self.assertEqual(
            provisioned_resources,
            [{"name": "resource1", "value": "value1"}, {"name": "resource2", "value": "value2"}],
        )

    def test_provisioned_resources_empty(self):
        self.git_service.domain_identifier = "valid-domain"
        self.git_service.project_identifier = "valid-project"
        self.mock_project_api.get_project_default_environment.return_value = {
            "provisionedResources": []
        }
        provisioned_resources = self.git_service._get_provisioned_resources()
        self.assertEqual(provisioned_resources, [])

    @patch.object(GitService, "_resolve_client")
    @patch.object(GitService, "_get_repository_id")
    def test_get_clone_url_client_error(self, mock_get_repository_id, mock_resolve_client):
        mock_git_client = Mock()
        mock_git_client.get_clone_url.side_effect = ClientError(
            {"Error": {"Code": "SomeError", "Message": "Some error occurred"}},
            "OperationName",
        )
        mock_resolve_client.return_value = mock_git_client
        mock_get_repository_id.return_value = "repository-id"
        with self.assertRaises(RuntimeError) as context:
            self.git_service.get_clone_url("dzd_bx2a2afyvn2hlc", "449et7665a6p3k")
            self.assertTrue(
                "Encountered error retrieving clone_url for project" in context.exception
            )
