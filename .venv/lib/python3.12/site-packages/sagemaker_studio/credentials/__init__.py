from dataclasses import dataclass
from datetime import datetime, timedelta

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from sagemaker_studio._openapi.models import GetDomainExecutionRoleCredentialsRequest
from sagemaker_studio.exceptions import AWSClientException
from sagemaker_studio.projects import ProjectService


@dataclass
class CredentialsVendingService:
    """
    Provides methods for retrieving AWS credentials for SageMaker Unified Studio projects and domains.

    Args:
        datazone_api (BaseClient): The DataZone client.
        project_api (ProjectService): The Project service.
    """

    def __init__(self, datazone_api: BaseClient, project_api: ProjectService):
        """
        Initializes a new instance of the CredentialsVendingService class.

        Args:
            datazone_api (BaseClient): The DataZone client.
            project_api (ProjectService): The Project service.
        """
        self.datazone_api: BaseClient = datazone_api
        self.project_api: ProjectService = project_api

    def get_project_default_environment_credentials(
        self, domain_identifier: str, project_identifier: str
    ) -> dict:
        """
        Retrieves the credentials for the default environment of a project.

        Args:
            domain_identifier (str): The unique identifier of the domain.
            project_identifier (str): The unique identifier of the project.

        Returns:
            dict: The AWS credentials for the default environment of the project.
        """
        try:
            project_default_environment = self.project_api.get_project_default_environment(
                domain_identifier=domain_identifier, project_identifier=project_identifier
            )
            return self.datazone_api.get_environment_credentials(  # type: ignore
                domainIdentifier=project_default_environment["domainId"],
                environmentIdentifier=project_default_environment["id"],
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise ValueError(f"Invalid input parameters: {AWSClientException(e)}")
            else:
                raise ValueError(
                    f"Unable to get default environment credentials: {AWSClientException(e)}"
                )

    def get_domain_execution_role_credential_in_space(self, domain_identifier: str):
        """
        Retrieves the domain execution role credentials for the specified domain.

        Args:
            domain_identifier (str): The unique identifier of the domain.

        Returns:
            dict: The AWS credentials for the domain execution role.
        """
        try:
            GetDomainExecutionRoleCredentialsRequest(domain_identifier)
        except Exception as e:
            raise e
        try:
            domain_exec_creds = self.datazone_api.get_domain_execution_role_credentials(  # type: ignore
                domainIdentifier=domain_identifier
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise ValueError(f"Invalid input parameters: {AWSClientException(e)}")
            else:
                raise AWSClientException(e)
        return {
            "Version": 1,
            "AccessKeyId": domain_exec_creds.get("credentials", {}).get("accessKeyId", ""),
            "SecretAccessKey": domain_exec_creds.get("credentials", {}).get("secretAccessKey", ""),
            "SessionToken": domain_exec_creds.get("credentials", {}).get("sessionToken"),
            "Expiration": domain_exec_creds.get("credentials", {}).get(
                "expiration", datetime.now() + timedelta(minutes=20)
            ),
        }
