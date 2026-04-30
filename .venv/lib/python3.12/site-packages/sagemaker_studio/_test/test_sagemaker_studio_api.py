import json
import os
import unittest
from typing import Dict, List
from unittest.mock import Mock, patch

from sagemaker_studio import ClientConfig
from sagemaker_studio.execution.remote_execution_client import RemoteExecutionClient
from sagemaker_studio.models.execution import ExecutionConfig
from sagemaker_studio.sagemaker_studio_api import SageMakerStudioAPI
from sagemaker_studio.utils import AIRFLOW_PREFIX


class TestSageMakerUIHelper(unittest.TestCase):
    def setUp(self):
        os.environ["DataZoneDomainId"] = "bogus_domain_id"
        os.environ["DataZoneProjectId"] = "bogus_project_id"
        os.environ["DataZoneEnvironmentId"] = "bogus_environment_id"
        os.environ["DataZoneDomainRegion"] = "us-east-1"
        client_config = ClientConfig(overrides={"execution": {"local": True}})
        self.sagemaker_studio = SageMakerStudioAPI(client_config)
        self.sagemaker_studio.datazone_api = Mock()
        self.sagemaker_studio.project_api = Mock()
        self.mock_paginators = {}
        self.sagemaker_studio.datazone_api.get_paginator.side_effect = self.get_mock_paginator
        with open(self.get_mock_sagemaker_space_metadata_path(), "w") as metadata_json:
            json.dump(self.get_mock_sagemaker_space_metadata(), metadata_json, indent=4)

    def tearDown(self):
        os.environ.pop("DataZoneDomainId", None)
        os.environ.pop("DataZoneProjectId", None)
        os.environ.pop("DataZoneEnvironmentId", None)
        os.environ.pop("DataZoneDomainRegion", None)
        os.environ.pop(f"{AIRFLOW_PREFIX}DataZoneDomainId", None)
        os.environ.pop(f"{AIRFLOW_PREFIX}DataZoneProjectId", None)
        os.environ.pop(f"{AIRFLOW_PREFIX}DataZoneEnvironmentId", None)

    def test_build_local_execution_config(self):
        execution_config = ExecutionConfig(local=True)
        execution_config.domain_identifier = "bogus_domain_id"
        execution_config.project_identifier = "bogus_project_id"
        execution_config.datazone_endpoint = "https://datazone.us-east-1.api.aws"
        execution_config.datazone_domain_region = "us-east-1"
        assert self.sagemaker_studio._build_execution_config() == execution_config

    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_default_tooling_environment")
    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_sagemaker_environment")
    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_sagemaker_client")
    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_ec2client")
    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_ssmclient")
    @patch.object(RemoteExecutionClient, "_RemoteExecutionClient__set_stack")
    def test_build_remote_execution_config(
        self,
        mock_set_stack,
        mock_set_ssmclient,
        mock_set_ec2client,
        mock_set_sagemaker_client,
        mock_set_sagemaker_environment,
        mock_set_default_tooling_environment,
    ):
        os.environ["ProjectS3Path"] = (
            "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev"
        )
        client_config = ClientConfig(overrides={"execution": {"local": False}})
        sagemaker_studio2 = SageMakerStudioAPI(client_config)
        execution_config = ExecutionConfig()
        execution_config.domain_identifier = "bogus_domain_id"
        execution_config.project_identifier = "bogus_project_id"
        execution_config.datazone_endpoint = "https://datazone.us-east-1.api.aws"
        execution_config.datazone_domain_region = "us-east-1"
        execution_config.project_s3_path = (
            "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev"
        )
        assert sagemaker_studio2._build_execution_config() == execution_config
        del os.environ["ProjectS3Path"]

    def setup_mock_paginator(self, paginator_name: str, pages: List[Dict[str, str]]):
        self.mock_paginators[paginator_name] = self.create_mock_paginator(pages)

    def get_mock_paginator(self, paginator_name: str):
        return self.mock_paginators[paginator_name]

    def create_mock_paginator(self, pages: List[Dict[str, str]]):
        class MockPageIterator:
            def __init__(self, pages):
                self.pages = pages

            def __iter__(self):
                return iter(self.pages)

        mock_paginator = Mock()
        mock_paginator.paginate.return_value = MockPageIterator(pages)
        return mock_paginator

    def get_mock_default_tooling_environment_summary(self):
        return {
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

    def get_mock_invalid_default_tooling_environment_summary(self):
        tooling_env_summary = self.get_mock_default_tooling_environment_summary()
        tooling_env_summary["provisionedResources"] = []
        return tooling_env_summary

    @staticmethod
    def get_mock_sagemaker_space_metadata_path():
        return "test_sagemaker_space_metadata.json"

    @staticmethod
    def get_mock_sagemaker_space_metadata():
        return {
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
                "DataZoneDomainRegion": "us-east-1",
                "DataZoneScopeName": "dev",
                "DataZoneStage": "prod",
                "DataZoneUserId": "bogus_user_id_sm_space",
                "PrivateSubnets": "bogus_subnet_sm_space",
                "ProjectS3Path": "s3://bogus_bucket_name/bogus_domain_id_sm_space/bogus_project_id_sm_space/dev",
                "SecurityGroup": "bogus_security_group_sm_space",
            },
            "ResourceArnCaseSensitive": "",
        }

    def get_mock_list_projects_pages(self):
        return [
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

    def get_mock_list_connections_pages(self):
        return [
            {
                "items": [
                    {
                        "name": "bogus_connection_name_00",
                        "connectionId": "bogus_connection_id_00",
                        "domainId": "bogus_domain_id_00",
                        "domainUnitId": "bogus_domain_unit_id_00",
                        "physicalEndpoints": [],
                        "type": "REDSHIFT",
                    },
                    {
                        "name": "bogus_connection_name_01",
                        "connectionId": "bogus_connection_id_01",
                        "domainId": "bogus_domain_id_00",
                        "domainUnitId": "bogus_domain_unit_id_00",
                        "physicalEndpoints": [],
                        "type": "WORKFLOWS_MWAA",
                    },
                ]
            },
            {
                "items": [
                    {
                        "name": "bogus_connection_name_02",
                        "connectionId": "bogus_connection_id_02",
                        "domainId": "bogus_domain_id_00",
                        "domainUnitId": "bogus_domain_unit_id_00",
                        "physicalEndpoints": [],
                        "type": "ATHENA",
                    }
                ]
            },
        ]

    def get_mock_connection_summary(self):
        return {
            "name": "bogus_connection_name_02",
            "connectionId": "bogus_connection_id_02",
            "domainId": "bogus_domain_id_00",
            "domainUnitId": "bogus_domain_unit_id_00",
            "environmentUserRole": "bogus_environment_user_role_02",
            "physicalEndpoints": [],
            "type": "ATHENA",
            "connectionCredentials": {
                "accessKeyId": "bogus_access_key",
                "secretAccessKey": "bogus_secret_access_key",
                "sessionToken": "bogus_session_token",
            },
        }

    def get_mock_list_environments_with_mlflow_pages(self):
        return [
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

    def get_mock_ml_experiments_environment_summary(self):
        return {
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

    def get_mock_invalid_ml_experiments_environment_summary(self):
        ml_experiments_env_summary = self.get_mock_ml_experiments_environment_summary()
        ml_experiments_env_summary["provisionedResources"] = []
        return ml_experiments_env_summary
