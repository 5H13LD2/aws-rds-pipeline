import unittest
from unittest.mock import Mock

from botocore.exceptions import ClientError

from sagemaker_studio.credentials import CredentialsVendingService
from sagemaker_studio.exceptions import AWSClientException


class TestCredentialsVendingService(unittest.TestCase):

    def setUp(self):
        self.mock_datazone_api = Mock()
        self.mock_project_api = Mock()
        self.credentials_service = CredentialsVendingService(
            self.mock_datazone_api, self.mock_project_api
        )

    def test_get_domain_execution_role_credential_in_space_validation_exception(self):
        error_response = {"Error": {"Code": "ValidationException", "Message": "Invalid parameters"}}
        self.mock_datazone_api.get_domain_execution_role_credentials.side_effect = ClientError(
            error_response, "GetDomainExecutionRoleCredentialInSpace"  # type: ignore
        )
        with self.assertRaises(ValueError) as context:
            self.credentials_service.get_domain_execution_role_credential_in_space("invalid-domain")
        self.assertEqual(
            "Invalid value for `domain_identifier`, must match regular expression `^dzd[-_][a-zA-Z0-9_-]{1,36}$`",
            str(context.exception),
        )

    def test_get_domain_execution_role_credentials_in_space_access_denied_exception(self):
        error_response = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "Failed to find the user profile name info",
            },
            "ResponseMetadata": {
                "RequestId": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                "HTTPStatusCode": 403,
                "HTTPHeaders": {
                    "x-amzn-requestid": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "content-type": "application/json",
                    "content-length": "0",
                    "date": "Fri, 01 Jan 1970 00:00:00 GMT",
                },
                "RetryAttempts": 0,
            },
        }
        self.mock_datazone_api.get_domain_execution_role_credentials.side_effect = ClientError(
            error_response, "GetDomainExecutionRoleCredentialInSpace"  # type: ignore
        )
        with self.assertRaises(AWSClientException) as context:
            self.credentials_service.get_domain_execution_role_credential_in_space("dzd_domain123")
            self.assertTrue("AccessDeniedException" in str(context.exception))

    def test_get_domain_execution_role_credentials_in_space_validation_exception(self):
        error_response = {
            "Error": {
                "Code": "ValidationException",
                "Message": "ValidationException",
            },
            "ResponseMetadata": {
                "RequestId": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                "HTTPStatusCode": 403,
                "HTTPHeaders": {
                    "x-amzn-requestid": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "content-type": "application/json",
                    "content-length": "0",
                    "date": "Fri, 01 Jan 1970 00:00:00 GMT",
                },
                "RetryAttempts": 0,
            },
        }
        self.mock_datazone_api.get_domain_execution_role_credentials.side_effect = ClientError(
            error_response, "GetDomainExecutionRoleCredentialInSpace"  # type: ignore
        )
        with self.assertRaises(ValueError) as context:
            self.credentials_service.get_domain_execution_role_credential_in_space("dzd_domain123")
            self.assertTrue("Invalid input parameters" in str(context.exception))

    def test_get_project_default_environment_credentials(self):
        expected_env_creds = {
            "accessKeyId": "123",
            "expiration": "1970",
            "secretAccessKey": "456",
            "sessionToken": "789",
        }

        def mock_get_project_default_environment(domain_identifier, project_identifier):
            if domain_identifier == "domain_123" and project_identifier == "project_123":
                return {
                    "awsAccountId": "1234567890",
                    "domainId": "domain_123",
                    "id": "environment_123",
                    "name": "bogus_project",
                }
            else:
                return {}

        def mock_get_environment_credentials(*args, **kwargs):
            domain_identifier = kwargs.get("domainIdentifier")
            environment_identifier = kwargs.get("environmentIdentifier")
            if domain_identifier == "domain_123" and environment_identifier == "environment_123":
                return expected_env_creds
            else:
                return {}

        self.mock_project_api.get_project_default_environment.side_effect = (
            mock_get_project_default_environment
        )
        self.mock_datazone_api.get_environment_credentials.side_effect = (
            mock_get_environment_credentials
        )

        assert (
            self.credentials_service.get_project_default_environment_credentials(
                "domain_123", "project_123"
            )
            == expected_env_creds
        )

    def test_get_project_default_environment_credentials_raises_other_exception(self):
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Could not find Project Default Environment",
            },
            "ResponseMetadata": {
                "RequestId": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                "HTTPStatusCode": 403,
                "HTTPHeaders": {
                    "x-amzn-requestid": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "content-type": "application/json",
                    "content-length": "0",
                    "date": "Fri, 01 Jan 1970 00:00:00 GMT",
                },
                "RetryAttempts": 0,
            },
        }

        self.mock_project_api.get_project_default_environment.side_effect = (
            lambda domain_identifier, project_identifier: {
                "awsAccountId": "1234567890",
                "domainId": "domain_123",
                "id": "environment_123",
                "name": "bogus_project",
            }
        )
        self.mock_datazone_api.get_environment_credentials.side_effect = ClientError(
            error_response, "GetEnvironmentCredentials"  # type: ignore
        )

        with self.assertRaises(ValueError) as context:
            self.credentials_service.get_project_default_environment_credentials(
                "domain_123", "project_123"
            )
            self.assertTrue(
                "Unable to get default environment credentials" in str(context.exception)
            )

    def test_get_project_default_environment_credentials_raises_validation_exception(self):
        error_response = {
            "Error": {
                "Code": "ValidationException",
                "Message": "ValidationException",
            },
            "ResponseMetadata": {
                "RequestId": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                "HTTPStatusCode": 403,
                "HTTPHeaders": {
                    "x-amzn-requestid": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "content-type": "application/json",
                    "content-length": "0",
                    "date": "Fri, 01 Jan 1970 00:00:00 GMT",
                },
                "RetryAttempts": 0,
            },
        }

        self.mock_project_api.get_project_default_environment.side_effect = (
            lambda domain_identifier, project_identifier: {
                "awsAccountId": "1234567890",
                "domainId": "domain_123",
                "id": "environment_123",
                "name": "bogus_project",
            }
        )
        self.mock_datazone_api.get_environment_credentials.side_effect = ClientError(
            error_response, "GetEnvironmentCredentials"  # type: ignore
        )

        with self.assertRaises(ValueError) as context:
            self.credentials_service.get_project_default_environment_credentials(
                "domain_123", "project_123"
            )
            self.assertTrue("Invalid input parameters" in str(context.exception))
