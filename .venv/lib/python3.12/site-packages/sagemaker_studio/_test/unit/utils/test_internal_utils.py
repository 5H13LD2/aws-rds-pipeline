import json
import os
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

import sagemaker_studio.utils
from sagemaker_studio.utils._internal import InternalUtils

MOCK_SAGEMAKER_METADATA_PATH = "test_sagemaker_space_metadata.json"
MOCK_SAGEMAKER_METADATA_CONTENT = {
    "AppType": "JupyterLab",
    "DomainId": "bogus_domain_id_sm_space",
    "SpaceName": "bogus_space_name_sm_space",
    "UserProfileName": "bogus_user_profile_name_sm_space",
    "ExecutionRoleArn": "bogus_execution_role_arn",
    "ResourceArn": "bogus_resource_arn_sm_space",
    "AdditionalMetadata": {
        "DataZoneDomainId": "bogus_domain_id_sm_space",
        "DataZoneEndpoint": "bogus_endpoint_sm_space",
        "DataZoneEnvironmentId": "bogus_environment_id_sm_space",
        "DataZoneProjectId": "bogus_project_id_sm_space",
        "DataZoneProjectRepositoryName": "bogus_repository_name_sm_space",
        "DataZoneDomainRegion": "test-domain-region-us-east-1",
        "DataZoneScopeName": "dev",
        "DataZoneStage": "prod",
        "DataZoneUserId": "bogus_user_id_sm_space",
        "PrivateSubnets": "bogus_subnet_sm_space",
        "ProjectS3Path": "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev",
        "SecurityGroup": "bogus_security_group_sm_space",
    },
    "ResourceArnCaseSensitive": "",
}
MOCK_LIST_PROJECT_PAGES = [
    {
        "items": [
            {
                "name": "bogus_project_name_00",
                "id": "bogus_project_id_00",
                "domainId": "bogus_domain_id_00",
            },
            {
                "name": "bogus_project_name_01",
                "id": "bogus_project_id_01",
                "domainId": "bogus_domain_id_00",
            },
        ]
    },
    {
        "items": [
            {
                "name": "bogus_project_name_02",
                "id": "bogus_project_id_02",
                "domainId": "bogus_domain_id_00",
            },
            {
                "name": "bogus_project_name_03",
                "id": "bogus_project_id_03",
                "domainId": "bogus_domain_id_00",
            },
        ]
    },
]

MOCK_PROJECT_DEFAULT_ENV_SUMMARY = {
    "awsAccountId": "1234567890",
    "awsAccountRegion": "us-east-1",
    "domainId": "bogus_domain_id",
    "environmentBlueprintId": "bogus_env_blueprint_id",
    "environmentProfileId": "bogus_env_profile_id",
    "id": "bogus_env_id",
    "name": "bogus_env_name",
    "projectId": "bogus_project_id",
    "provider": "Amazon SageMaker",
    "provisionedResources": [
        {
            "name": "s3BucketPath",
            "provider": "Amazon SageMaker",
            "type": "string",
            "value": "bogus_s3_bucket_path",
        },
        {
            "name": "kmsKeyArn",
            "provider": "Amazon SageMaker",
            "type": "string",
            "value": "bogus_project_kms_key_arn",
        },
        {
            "name": "otherProvisionedResource",
            "provider": "Amazon SageMaker",
            "type": "string",
            "value": "bogus_provisioned_resource_value",
        },
    ],
    "status": "ACTIVE",
}

MOCK_LIST_ENVIRONMENTS_PAGINATOR = [
    {
        "items": [
            {
                "name": "bogus_environment_name_00",
                "id": "bogus_environment_id_00",
                "domainId": "bogus_domain_id_00",
                "projectId": "bogus_project_id_03",
            },
            {
                "name": "bogus_environment_name_01",
                "id": "bogus_environment_id_01",
                "domainId": "bogus_domain_id_00",
                "projectId": "bogus_project_id_03",
            },
        ]
    },
    {
        "items": [
            {
                "name": "bogus_environment_name_02",
                "id": "bogus_environment_id_02",
                "domainId": "bogus_domain_id_00",
                "projectId": "bogus_project_id_03",
            },
            {
                "name": "MLExperiments",
                "id": "bogus_environment_id_03",
                "domainId": "bogus_domain_id_00",
                "projectId": "bogus_project_id_03",
            },
        ]
    },
]


@patch.object(
    sagemaker_studio.utils, "SAGEMAKER_METADATA_JSON_PATH", "test_sagemaker_space_metadata.json"
)
class TestInternalUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(MOCK_SAGEMAKER_METADATA_PATH, "w") as metadata_json:
            json.dump(MOCK_SAGEMAKER_METADATA_CONTENT, metadata_json, indent=4)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(MOCK_SAGEMAKER_METADATA_PATH):
            os.remove(MOCK_SAGEMAKER_METADATA_PATH)

    def setUp(self):
        self.utils = InternalUtils()

    def test_get_domain_region_from_sagemaker_metadata(self):
        region = self.utils._get_domain_region()
        self.assertEqual(region, "test-domain-region-us-east-1")

    @patch("json.load")
    def test_get_domain_region_from_environment_variable(self, json_load_mock: Mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)

        os.environ["DataZoneDomainRegion"] = "os-environ-domain-region"
        region = self.utils._get_domain_region()

        self.assertEqual(region, "os-environ-domain-region")
        del os.environ["DataZoneDomainRegion"]

    @patch("json.load")
    def test_get_domain_region_from_mwaa_space_environment_variable(self, json_load_mock: Mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)

        os.environ["AIRFLOW__WORKFLOWS__DATAZONE_DOMAIN_REGION"] = "os-environ-mwaa-domain-region"
        region = self.utils._get_domain_region()

        self.assertEqual(region, "os-environ-mwaa-domain-region")
        del os.environ["AIRFLOW__WORKFLOWS__DATAZONE_DOMAIN_REGION"]

    @patch("json.load")
    def test_get_domain_region_from_default(self, json_load_mock: Mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)
        region = self.utils._get_domain_region()

        self.assertEqual(region, "")

    def test_get_project_id_domain_id_not_specified(self):
        with self.assertRaises(ValueError) as context:
            self.utils._get_project_id(Mock(), project_name="bogus_project_name")
        self.assertIn(
            "If specifying project name, a domain ID must also be specified.",
            str(context.exception),
        )

    @patch("json.load")
    def test_get_project_id_from_environment_variable(self, json_load_mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)
        os.environ["DataZoneProjectId"] = "bogus_project_id"
        self.assertEqual(self.utils._get_project_id(Mock()), "bogus_project_id")
        del os.environ["DataZoneProjectId"]

    def test_get_project_id_from_sagemaker_metadata_json(self):
        self.assertEqual(self.utils._get_project_id(Mock()), "bogus_project_id_sm_space")

    def test_get_project_id_from_datazone_api(self):
        list_project_paginator = self.create_mock_paginator(MOCK_LIST_PROJECT_PAGES)
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_project_paginator

        self.assertEqual(
            self.utils._get_project_id(
                datazone_api, domain_id="bogus_domain_id_00", project_name="bogus_project_name_02"
            ),
            "bogus_project_id_02",
        )

    def test_get_project_id_from_datazone_api_invalid_project(self):
        list_project_paginator = self.create_mock_paginator(MOCK_LIST_PROJECT_PAGES)
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_project_paginator

        with self.assertRaises(RuntimeError) as context:
            self.utils._get_project_id(
                datazone_api, domain_id="bogus_domain_id_00", project_name="invalid_project_name"
            )
        self.assertIn("Project invalid_project_name not found", str(context.exception))

    @patch("json.load")
    def test_get_project_id_from_sagemaker_metadata_json_not_found(self, json_load_mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)
        with self.assertRaises(ValueError) as context:
            self.utils._get_project_id(Mock(), "bogus_project_id_sm_space")
            self.assertTrue("Project ID not found in environment." in context.exception)

    def test_get_datazone_env_id(self):
        env_id = self.utils._get_datazone_environment_id()
        self.assertEqual(env_id, "bogus_environment_id_sm_space")

    def test_get_user_id(self):
        user_id = self.utils._get_user_id()
        self.assertEqual(user_id, "bogus_user_id_sm_space")

    def test_get_user_id_not_found(self):
        self.utils._get_field_from_sagemaker_space_metadata_json = Mock()
        self.utils._get_field_from_sagemaker_space_metadata_json.return_value = ""
        with self.assertRaises(RuntimeError) as context:
            self.utils._get_user_id()
            self.assertTrue("Encountered an error getting the current user ID" in context.exception)

    def test_get_datazone_stage(self):
        datazone_stage = self.utils._get_datazone_stage()
        self.assertEqual(datazone_stage, "prod")

    def test_get_datazone_endpoint(self):
        dz_endpoint = self.utils._get_datazone_endpoint("test-region-1")
        self.assertEqual(dz_endpoint, "bogus_endpoint_sm_space")

    @patch("json.load")
    def test_get_datazone_endpoint_defaults_to_prod(self, json_load_mock):
        json_load_mock.side_effect = json.JSONDecodeError("Error loading file", "Doc", 0)
        datazone_endpoint = self.utils._get_datazone_endpoint(region="test-region-1")
        self.assertEqual(datazone_endpoint, "https://datazone.test-region-1.api.aws")

    def test_get_project_kms_key_arn_invalid_project_params(self):
        with self.assertRaises(RuntimeError) as context:
            self.utils._get_project_kms_key_arn(
                project_api=Mock(),
                datazone_api=Mock(),
                domain_id="dzd_1234",
            )
            self.assertTrue("Encountered an error getting the project KMS key" in context.exception)

    def test_get_project_kms_key_arn_project_id_from_param(self):
        with patch.object(self.utils, "_get_default_tooling_environment_summary") as mock:
            mock.return_value = MOCK_PROJECT_DEFAULT_ENV_SUMMARY
            kms_key_arn = self.utils._get_project_kms_key_arn(
                project_api=Mock(),
                datazone_api=Mock(),
                domain_id="dzd_1234",
                project_id="abcd12345",
            )
            self.assertEqual(kms_key_arn, "bogus_project_kms_key_arn")

    def test_get_project_kms_key_arn_project_id_from_env(self):
        with patch.object(self.utils, "_get_default_tooling_environment_summary") as mock:
            mock.return_value = MOCK_PROJECT_DEFAULT_ENV_SUMMARY
            kms_key_arn = self.utils._get_project_kms_key_arn(
                project_api=Mock(),
                datazone_api=Mock(),
                domain_id="dzd_1234",
            )
            self.assertEqual(kms_key_arn, "bogus_project_kms_key_arn")

    def test_get_project_kms_key_arn_not_in_provisioned_resources(self):
        with patch.object(self.utils, "_get_default_tooling_environment_summary") as mock:
            mock.return_value = {
                "awsAccountId": "1234567890",
                "awsAccountRegion": "us-east-1",
                "domainId": "bogus_domain_id",
                "environmentBlueprintId": "bogus_env_blueprint_id",
                "environmentProfileId": "bogus_env_profile_id",
                "id": "bogus_env_id",
                "name": "bogus_env_name",
                "projectId": "bogus_project_id",
                "provider": "Amazon SageMaker",
                "provisionedResources": [
                    {
                        "name": "s3BucketPath",
                        "provider": "Amazon SageMaker",
                        "type": "string",
                        "value": "bogus_s3_bucket_path",
                    }
                ],
            }

            with self.assertRaises(RuntimeError) as context:
                self.utils._get_project_kms_key_arn(
                    project_api=Mock(),
                    datazone_api=Mock(),
                    domain_id="dzd_1234",
                )
                self.assertTrue(
                    "kmsKeyArn provisioned resource not found in Tooling environment"
                    in context.exception
                )

    def test_get_mlflow_tracking_server_arn_validation_fails(self):
        with patch.object(self.utils, "_get_project_id") as mock:
            mock.return_value = ""
            with self.assertRaises(ValueError) as context:
                self.utils._get_mlflow_tracking_server_arn(
                    datazone_api=Mock(),
                    domain_id="dzd_124",
                )
                self.assertTrue(
                    "One of project_id or project_name must be provided" in context.exception
                )

    def test_get_mlflow_tracking_server_arn_project_id_from_env(self):
        list_env_paginator = self.create_mock_paginator(MOCK_LIST_ENVIRONMENTS_PAGINATOR)
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_env_paginator
        datazone_api.get_environment.return_value = {
            "awsAccountId": "1234567890",
            "awsAccountRegion": "us-east-1",
            "domainId": "bogus_domain_id_00",
            "environmentBlueprintId": "bogus_env_blueprint_id",
            "environmentProfileId": "bogus_env_profile_id",
            "id": "bogus_environment_id_03",
            "name": "MLExperiments",
            "projectId": "bogus_project_id_03",
            "provider": "Amazon SageMaker",
            "provisionedResources": [
                {
                    "name": "mlflowTrackingServerArn",
                    "provider": "Amazon SageMaker",
                    "type": "string",
                    "value": "bogus_mlflow_tracking_server_arn",
                },
            ],
            "status": "ACTIVE",
        }

        mlflow_arn = self.utils._get_mlflow_tracking_server_arn(
            datazone_api=datazone_api,
            domain_id="dzd_!234",
        )
        self.assertEqual(mlflow_arn, "bogus_mlflow_tracking_server_arn")

    def test_get_mlflow_tracking_server_arn_throws_error_in_list_env(self):
        list_env_paginator = self.create_mock_paginator(
            {
                "items": [
                    {
                        "name": "bogus_environment_name_00",
                        "id": "bogus_environment_id_00",
                        "domainId": "bogus_domain_id_00",
                        "projectId": "bogus_project_id_03",
                    }
                ]
            }
        )
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_env_paginator

        with self.assertRaises(RuntimeError) as context:
            self.utils._get_mlflow_tracking_server_arn(
                datazone_api=datazone_api,
                domain_id="dzd_!234",
            )
            self.assertTrue("MLExperiments environment not found" in context.exception)

    def test_get_mlflow_tracking_server_arn_throws_error(self):
        list_env_paginator = self.create_mock_paginator(MOCK_LIST_ENVIRONMENTS_PAGINATOR)
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_env_paginator
        datazone_api.get_environment.side_effect = Exception()

        with self.assertRaises(RuntimeError) as context:
            self.utils._get_mlflow_tracking_server_arn(
                datazone_api=datazone_api,
                domain_id="dzd_!234",
            )
            self.assertTrue("Encountered an error getting ML flow tracking" in context.exception)

    def test_get_mlflow_tracking_server_arn_no_arn(self):
        list_env_paginator = self.create_mock_paginator(MOCK_LIST_ENVIRONMENTS_PAGINATOR)
        datazone_api = Mock()
        datazone_api.get_paginator.return_value = list_env_paginator
        datazone_api.get_environment.return_value = {
            "provisionedResources": [],
            "status": "ACTIVE",
        }

        with self.assertRaises(RuntimeError) as context:
            self.utils._get_mlflow_tracking_server_arn(
                datazone_api=datazone_api,
                domain_id="dzd_!234",
            )
            self.assertTrue("Encountered an error getting ML flow tracking" in context.exception)

    def test_get_project_s3_path_validation_fails_available_in_env(self):
        with patch.object(self.utils, "_get_project_id") as mock_get_project:
            mock_get_project.return_value = ""
            with patch.object(
                self.utils, "_get_default_tooling_environment_summary"
            ) as mock_tooling:
                mock_tooling.return_value = MOCK_PROJECT_DEFAULT_ENV_SUMMARY
                s3_path = self.utils._get_project_s3_path(
                    project_api=Mock(),
                    datazone_api=Mock(),
                    domain_id="dzd_1234",
                )
                self.assertEqual(
                    s3_path,
                    "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev",
                )

    def test_get_project_s3_path_validation_fails_not_available_in_env(self):
        with patch.object(self.utils, "_get_project_id") as mock_project_id:
            mock_project_id.return_value = ""
            with patch.object(self.utils, "_get_field_from_environment") as mock_from_env:
                mock_from_env.return_value = ""
                with self.assertRaises(RuntimeError) as context:
                    self.utils._get_project_s3_path(
                        project_api=Mock(),
                        datazone_api=Mock(),
                        domain_id="dzd_1234",
                    )
                    self.assertTrue(
                        "Encountered an error getting the project S3 path" in context.exception
                    )

    def test_get_project_s3_path_not_in_tooling_env(self):
        with patch.object(self.utils, "_get_project_id") as mock_get_project:
            mock_get_project.return_value = ""
            with patch.object(
                self.utils, "_get_default_tooling_environment_summary"
            ) as mock_tooling:
                mock_tooling.return_value = {
                    "provisionedResources": [
                        {
                            "name": "mlflowTrackingServerArn",
                            "provider": "Amazon SageMaker",
                            "type": "string",
                            "value": "bogus_mlflow_tracking_server_arn",
                        },
                    ],
                    "status": "ACTIVE",
                }
            s3_path = self.utils._get_project_s3_path(
                project_api=Mock(),
                datazone_api=Mock(),
                domain_id="dzd_1234",
            )
            self.assertEqual(
                s3_path,
                "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev",
            )

    def create_mock_paginator(self, pages):
        class MockPageIterator:
            def __init__(self, pages):
                self.pages = pages

            def __iter__(self):
                return iter(self.pages)

        mock_paginator = Mock()
        mock_paginator.paginate.return_value = MockPageIterator(pages)
        return mock_paginator

    def test_get_tooling_environment_summary_throws_client_error(self):
        project_api = Mock()
        project_api.get_project_default_environment.return_value = {"id": "bogus_env_id"}
        datazone_api = Mock()
        datazone_api.get_environment.side_effect = ClientError(
            error_response={"Error": {"Code": "ValidationException"}},
            operation_name="GetEnvironment",
        )
        with self.assertRaises(RuntimeError) as context:
            self.utils._get_default_tooling_environment_summary(
                project_api=project_api,
                datazone_api=datazone_api,
                domain_id="XXXXXXXX",
                project_id="XXXXXXXXX",
            )
            self.assertTrue(
                "Encountered error getting project default tooling environment for project"
                in context.exception
            )
