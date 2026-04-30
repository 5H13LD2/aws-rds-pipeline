import unittest
from unittest.mock import Mock

from botocore.exceptions import ClientError

from sagemaker_studio.exceptions import AWSClientException
from sagemaker_studio.projects import ProjectService


class TestProjectService(unittest.TestCase):
    def setUp(self):
        self.mock_datazone_api = Mock()
        self.project_service = ProjectService(self.mock_datazone_api)

    def test_is_default_environment_present(self):
        self.mock_datazone_api.list_environment_blueprints.return_value = {
            "items": [{"id": "test_environment_id"}]
        }
        self.mock_datazone_api.list_environments.return_value = {
            "items": [{"id": "test_environment_id"}]
        }
        result = self.project_service.is_default_environment_present(
            domain_identifier="test_domain",
            project_identifier="test_project",
            blueprint_name="DataLake",
        )
        self.assertTrue(result)

    def test_is_default_environment_present_no_blueprints(self):
        self.mock_datazone_api.list_environment_blueprints.return_value = {"items": []}
        with self.assertRaises(ValueError) as context:
            self.project_service.is_default_environment_present(
                domain_identifier="test_domain",
                project_identifier="test_project",
                blueprint_name="DataLake",
            )
        self.assertIn("DataLake environment blueprint not found", str(context.exception))

    def test_is_default_environment_present_no_environments(self):
        self.mock_datazone_api.list_environment_blueprints.return_value = {
            "items": [{"id": "blueprint_id_123"}]
        }
        self.mock_datazone_api.list_environments.return_value = {"items": []}
        result = self.project_service.is_default_environment_present(
            domain_identifier="test_domain",
            project_identifier="test_project",
            blueprint_name="DataLake",
        )
        self.assertFalse(result)

    def test_get_project_default_environment_throws_validation_exception(self):
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
        self.mock_datazone_api.list_environment_blueprints.return_value = {
            "items": [{"id": "blueprint_id_123", "name": "Tooling"}]
        }
        self.mock_datazone_api.list_environments.side_effect = ClientError(
            error_response, "ListEnvironments"
        )

        with self.assertRaises(ValueError) as context:
            self.project_service.get_project_default_environment("dzd_1234", "abc1244")
            self.assertTrue("Invalid input parameters" in context.exception)

    def test_get_project_default_environment_throws_other_exception(self):
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "The environment blueprint could not be found",
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
        self.mock_datazone_api.list_environment_blueprints.return_value = {
            "items": [{"id": "blueprint_id_123", "name": "Tooling"}]
        }
        self.mock_datazone_api.list_environments.side_effect = ClientError(
            error_response, "ListEnvironments"
        )

        with self.assertRaises(AWSClientException) as context:
            self.project_service.get_project_default_environment("dzd_1234", "abc1244")
            self.assertTrue("ResourceNotFoundException" in context.exception)

    def test_get_project_sagemaker_environment_throws_value_error(self):
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
        self.mock_datazone_api.list_environments.side_effect = ClientError(
            error_response, "ListEnvironments"
        )

        with self.assertRaises(ValueError) as context:
            self.project_service.get_project_sagemaker_environment("dzd_1234", "abc1244")
            self.assertTrue("Invalid input parameters" in context.exception)

    def test_get_project_sagemaker_environment_throws_other_error(self):
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "ResourceNotFoundException",
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
        self.mock_datazone_api.list_environments.side_effect = ClientError(
            error_response, "ListEnvironments"
        )

        with self.assertRaises(AWSClientException) as context:
            self.project_service.get_project_sagemaker_environment("dzd_1234", "abc1244")
            self.assertTrue("ResourceNotFoundException" in context.exception)

    def test_get_project_default_environment_throws_value_error_when_no_tooling_blueprint(self):
        self.mock_datazone_api.list_environment_blueprints.return_value = {"items": []}

        with self.assertRaises(ValueError) as context:
            self.project_service.get_project_default_environment("dzd_1234", "abc1244")
            self.assertTrue("Tooling environment blueprint not found" in context.exception)

    def test_get_project_default_environment_regular_domain(self):
        self.mock_datazone_api.list_environment_blueprints.return_value = {
            "items": [{"id": "blueprint_id_123", "name": "Tooling"}]
        }

        self.mock_datazone_api.list_environments.return_value = {
            "items": [{"deploymentOrder": 0, "name": "Tooling", "id": "tooling_1234"}]
        }

        default_env = self.project_service.get_project_default_environment("dzd_1234", "abc1244")

        self.assertTrue(default_env["id"], "tooling_1234")

        self.mock_datazone_api.list_environment_blueprints.assert_called_once()
        self.mock_datazone_api.list_environments.assert_called_once()
        self.mock_datazone_api.list_environment_blueprints.assert_called_once_with(
            domainIdentifier="dzd_1234", managed=True, name="Tooling", provider="Amazon SageMaker"
        )
        self.mock_datazone_api.list_environments.assert_called_once_with(
            domainIdentifier="dzd_1234",
            projectIdentifier="abc1244",
            environmentBlueprintIdentifier="blueprint_id_123",
        )

    def test_get_project_default_environment_express_domain(self):
        self.mock_datazone_api.list_environment_blueprints.side_effect = [
            {
                "items": [
                    {"id": "blueprint_id_123", "name": "Tooling"},
                    {"id": "blueprint_id_321", "name": "ToolingLite"},
                ]
            },
            {"items": [{"id": "blueprint_id_321", "name": "ToolingLite"}]},
        ]

        self.mock_datazone_api.list_environments.side_effect = [
            {"items": []},
            {"items": [{"deploymentOrder": 0, "name": "ToolingLite", "id": "tooling_lite_1234"}]},
        ]

        default_env = self.project_service.get_project_default_environment("dzd_1234", "abc1244")

        self.assertTrue(default_env["id"], "tooling_lite_1234")

        self.assertEqual(self.mock_datazone_api.list_environment_blueprints.call_count, 2)
        self.assertEqual(self.mock_datazone_api.list_environments.call_count, 2)
        self.mock_datazone_api.list_environment_blueprints.assert_called_with(
            domainIdentifier="dzd_1234",
            managed=True,
            name="ToolingLite",
            provider="Amazon SageMaker",
        )

    def test_get_project_default_environment_tooling_lite_blueprint_value_error(self):
        self.mock_datazone_api.list_environment_blueprints.side_effect = [
            {
                "items": [
                    {"id": "blueprint_id_123", "name": "Tooling"},
                    {"id": "blueprint_id_321", "name": "ToolingLite"},
                ]
            },
            {"items": []},
        ]

        self.mock_datazone_api.list_environments.side_effect = [{"items": []}]

        with self.assertRaises(ValueError) as context:
            self.project_service.get_project_default_environment("dzd_1234", "abc1244")
            self.assertTrue("ToolingLite environment blueprint not found" in context.exception)

    def test_get_project_default_environment_tooling_lite_default_env_not_found(self):
        self.mock_datazone_api.list_environment_blueprints.side_effect = [
            {
                "items": [
                    {"id": "blueprint_id_123", "name": "Tooling"},
                    {"id": "blueprint_id_321", "name": "ToolingLite"},
                ]
            },
            {"items": [{"id": "blueprint_id_321", "name": "ToolingLite"}]},
        ]

        self.mock_datazone_api.list_environments.side_effect = [
            {"items": []},
            {"items": []},
        ]

        with self.assertRaises(ValueError) as context:
            self.project_service.get_project_default_environment("dzd_1234", "abc1244")
            self.assertTrue("ToolingLite environment  not found" in context.exception)
