from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field, make_dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from sagemaker_studio.data_models import ClientConfig
from sagemaker_studio.exceptions import AWSClientException

from .catalog import Catalog


@dataclass
class PhysicalEndpoint:
    """
    Represents the physical endpoint details of a connection.

    Attributes:
        access_role (Optional[str]): The IAM role associated with the physical endpoint.
        aws_account_id (Optional[str]): The AWS account ID associated with the physical endpoint.
        aws_region (Optional[str]): The AWS region associated with the physical endpoint.
        iam_connection_id (Optional[str]): The IAM connection ID associated with the physical endpoint.
        glue_connection_name (Optional[str]): The Glue connection name associated with the physical endpoint.
        host (Optional[str]): The host of the physical endpoint.
        port (Optional[str]): The port of the physical endpoint.
        protocol (Optional[str]): The protocol of the physical endpoint.
        stage (Optional[str]): The stage of the physical endpoint.
    """

    _connection_instance: Connection
    access_role: Optional[str] = None
    aws_account_id: Optional[str] = None
    aws_region: Optional[str] = None
    iam_connection_id: Optional[str] = None
    glue_connection_name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    protocol: Optional[str] = None
    stage: Optional[str] = None
    _glue_connection: Optional[Dict] = field(default=None)

    @property
    def glue_connection(self) -> Optional[Dict]:
        """
        Returns the Glue connection associated with this physical endpoint.
        """
        if self._glue_connection is None:
            self._connection_instance._invoke_get_connection_and_populate_fields()
            if self._connection_instance.physical_endpoints:
                return self._connection_instance.physical_endpoints[0]._glue_connection
        return self._glue_connection

    def __str__(self) -> str:
        """
        Returns a string representation of the PhysicalEndpoint object.

        This method creates a string containing all non-null attributes of the PhysicalEndpoint object.
        It excludes any attributes with None values.

        Returns:
            str: A string representation of the PhysicalEndpoint object in the format:
                 "PhysicalEndpoint(attribute1='value1', attribute2='value2', ...)"
        """
        return self.__repr__()

    def __repr__(self) -> str:
        """
        Returns a string representation of the PhysicalEndpoint object.

        This method creates a string containing all non-null attributes of the PhysicalEndpoint object.
        It excludes any attributes with None values.

        Returns:
            str: A string representation of the PhysicalEndpoint object in the format:
                 "PhysicalEndpoint(attribute1='value1', attribute2='value2', ...)"
        """
        non_null_fields = []
        for key, value in self.__dict__.items():
            if not key.startswith("_") and value is not None:
                non_null_fields.append(f"{key}='{value}'")
        if self._glue_connection is not None:
            non_null_fields.append(f"glue_connection='{self._glue_connection}'")
        return f"PhysicalEndpoint({', '.join(non_null_fields)})"


@dataclass
class ConnectionCredentials:
    """
    Represents the connection credentials details of a connection.

    Attributes:
        access_key_id (Optional[str]): The access key ID associated with the connection credentials.
        secret_access_key (Optional[str]): The secret access key associated with the connection credentials.
        session_token (Optional[str]): The session token associated with the connection credentials.
        expiration (Optional[str]): The expiration associated with the connection credentials.
    """

    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    expiration: Optional[str] = None


@dataclass
class Connection:
    """
    Represents a connection within a SageMaker Unified Studio Project. Connection allows a user to retrieve information
    about a connection, including its name, databases, IAM role (if applicable), physical endpoints, username/password,
    JDBC URL (if applicable), and more.

    Attributes:
        name (Optional[str]): The name of the connection.
        type (Optional[str]): The type of the connection.
        id (Optional[str]): The unique identifier of the connection.
        connection_creds (Optional[ConnectionCredentials]): The connection credentials associated with the connection.
        domain_id (Optional[str]): The unique identifier of the domain the connection belongs to.
        domain_unit_id (Optional[str]): The unique identifier of the domain unit the connection belongs to.
        environment_id (Optional[str]): The unique identifier of the environment the connection belongs to.
        physical_endpoints (Optional[List[PhysicalEndpoint]]): The physical endpoints associated with the connection.
        project_id (Optional[str]): The unique identifier of the project the connection belongs to.
        iam_role (Optional[str]): The IAM role associated with the connection.

    Args:
        connection_data (Dict[str, Any]): The raw connection data.
        glue_api (BaseClient): The Glue client.
    """

    __connection_data: Dict[str, Any] = field(init=False, repr=False)
    name: Optional[str] = None
    type: Optional[str] = None
    id: Optional[str] = None
    _connection_creds: Optional[ConnectionCredentials] = field(default=None, repr=False)
    domain_id: Optional[str] = None
    domain_unit_id: Optional[str] = None
    environment_id: Optional[str] = None
    physical_endpoints: Optional[List[PhysicalEndpoint]] = None
    project_id: Optional[str] = None
    iam_role: Optional[str] = None

    def __init__(
        self,
        connection_data: Dict[str, Any],
        glue_api: BaseClient,
        datazone_api: BaseClient,
        secrets_manager_api: BaseClient,
        project_config: ClientConfig,
    ):
        """
        Initializes a new instance of the Connection class.

        Args:
            connection_data (Dict[str, Any]): The raw connection data.
            glue_api (BaseClient): The Glue client.
            datazone_api (BaseClient): The DataZone client.
        """
        self._datazone_api = datazone_api
        self.__connection_data = connection_data
        self.name = self.__connection_data.get("name", None)
        self.type = self.__connection_data.get("type", None)
        self.id = self.__connection_data.get("connectionId", None)
        connection_credentials = self.__connection_data.get("connectionCredentials", None)
        self._connection_creds = self._map_connection_credentials_field(connection_credentials)
        self.domain_id = self.__connection_data.get("domainId", None)
        self.domain_unit_id = self.__connection_data.get("domainUnitId", None)
        self.environment_id = self.__connection_data.get("environmentId", None)
        physical_endpoints_data = self.__connection_data.get("physicalEndpoints", [])
        self.physical_endpoints = [
            self._map_physical_endpoint_fields(endpoint) for endpoint in physical_endpoints_data
        ]
        self.project_id = self.__connection_data.get("projectId", None)
        self.iam_role = self.__connection_data.get("environmentUserRole", None)
        self._project_config = project_config
        if self.type in ["LAKEHOUSE", "IAM"]:
            self._glue_api: BaseClient = self._get_aws_client_with_connection_credentials(
                "glue", self._connection_creds, glue_api
            )
        self._secrets_manager_api = secrets_manager_api

    def __str__(self) -> str:
        """
        Returns a string representation of the Connection object.

        This method creates a string containing all non-null attributes of the Connection object.
        It excludes any private attributes and attributes with None values.

        Returns:
            str: A string representation of the Connection object in the format:
                 "Connection(attribute1='value1', attribute2='value2', ...)"
        """
        return self.__repr__()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Connection object.

        This method creates a string containing all non-null attributes of the Connection object.
        It excludes any private attributes and attributes with None values.

        Returns:
            str: A string representation of the Connection object in the format:
                 "Connection(attribute1='value1', attribute2='value2', ...)"
        """
        non_null_fields = []
        for key, value in self.__dict__.items():
            if not key.startswith("_") and value is not None:
                if isinstance(value, list) and len(value) > 0:
                    non_null_fields.append(f"{key}={value}")
                elif isinstance(value, str):
                    non_null_fields.append(f"{key}='{value}'")
        return f"Connection({', '.join(non_null_fields)})"

    @property
    def connection_creds(self):
        if self._connection_creds is None:
            self._invoke_get_connection_and_populate_fields()
        return self._connection_creds

    @property
    def data(self):
        """
        Retrieves all the detailed connection data as a ConnectionData object.

        Returns:
            ConnectionData: The connection data.
        """
        return self._create_connection_data(self.__connection_data)

    @property
    def catalogs(self) -> List[Catalog]:
        """
        Retrieves all catalogs for IAM connections.

        Returns:
            List[Catalog]: A list of all available catalogs.

        Raises:
            AttributeError: If catalogs are not available for this connection type.
            RuntimeError: If there's an error retrieving the catalogs.
        """
        if self.type not in ["LAKEHOUSE", "IAM"]:
            raise AttributeError("Catalogs are only available for IAM or LAKEHOUSE connections")

        try:
            connection_catalogs: List[Catalog] = []
            next_token = None
            first_call = True
            while next_token is not None or first_call:
                first_call = False
                params: Dict[str, Any] = {"HasDatabases": True, "Recursive": True}
                if next_token:
                    params["NextToken"] = next_token
                get_catalogs_response = self._glue_api.get_catalogs(**params)  # type: ignore
                for catalog in get_catalogs_response.get("CatalogList", []):
                    connection_catalogs.append(self._catalog_builder(catalog))
                next_token = get_catalogs_response.get("NextToken")
            return connection_catalogs
        except ClientError as e:
            raise RuntimeError(
                "Encountered an error getting catalogs for the connection", AWSClientException(e)
            )
        except Exception as e:
            raise RuntimeError(
                "Encountered an unexpected exception getting catalogs for the connection", e
            )

    def catalog(self, id: Optional[str] = None) -> Catalog:
        """
        Retrieves a specific catalog for IAM connections.

        Args:
            id (Optional[str]): The identifier of the catalog. Defaults to the connections default Catalog identified by its accountId

        Returns:
            Catalog: The requested catalog.

        Raises:
            AttributeError: If catalogs are not available for this connection type.
            RuntimeError: If there's an error retrieving the catalog.
        """
        if self.type not in ["LAKEHOUSE", "IAM"]:
            raise AttributeError("Catalogs are only available for IAM or LAKEHOUSE connections")

        aws_account_id = None
        # AwsAccountId is required only when id is None or id is not prefixed with AwsAccountId
        if not id or not self._validate_catalog_id(id):
            if self.physical_endpoints is None or len(self.physical_endpoints) == 0:
                raise RuntimeError("No physical endpoints found for the connection")
            aws_account_id = self.physical_endpoints[0].aws_account_id  # type: ignore

        try:
            if not id:
                get_catalog_response = self._glue_api.get_catalog(CatalogId=aws_account_id).get("Catalog")  # type: ignore
                return self._catalog_builder(get_catalog_response)

            if not self._validate_catalog_id(id):
                id = f"{aws_account_id}:{id}" if not self._validate_aws_account_id(id) else id

            get_catalog_response = self._glue_api.get_catalog(CatalogId=id).get("Catalog")  # type: ignore
            return self._catalog_builder(get_catalog_response)
        except ClientError as e:
            raise AttributeError(
                f"Unable to access catalog '{id or aws_account_id}': {AWSClientException(e)}"
            )
        except Exception as e:
            raise RuntimeError(f"Encountered an error getting catalog '{id or aws_account_id}", e)

    @property
    def secret(self) -> Union[Dict[str, str], str]:
        """
        Retrieves the secret associated with the connection from
        SecretsManager.

        Returns:
            Dict[str, Any] | str: The secret associated with the connection.

        Raises:
            RuntimeError: If there's an error retrieving the secret.
        """
        if len(self.physical_endpoints) >= 1 and not self.physical_endpoints[0].glue_connection:  # type: ignore
            self._invoke_get_connection_and_populate_fields()
        if len(self.physical_endpoints) >= 1 and self.physical_endpoints[0].glue_connection:  # type: ignore
            self._secrets_manager_api: BaseClient = self._get_aws_client_with_connection_credentials(  # type: ignore
                "secretsmanager", self._connection_creds, self._secrets_manager_api
            )
            secret_arn = self._find_secret_arn()
            try:
                secret = self._secrets_manager_api.get_secret_value(SecretId=secret_arn)  # type: ignore
            except ClientError as e:
                raise RuntimeError(
                    f"Encountered an error getting secret for the connection:{self.name} {AWSClientException(e)}"
                )

            return self._parse_secret(secret)
        else:
            if hasattr(self.data, "credentials") and self.data.credentials.get("usernamePassword"):
                credentials = self.data.credentials.get("usernamePassword")
                if (
                    credentials
                    and isinstance(credentials, dict)
                    and credentials.get("username")
                    and credentials.get("password")
                ):
                    return {
                        "username": credentials.get("username"),
                        "password": credentials.get("password"),
                    }
            raise RuntimeError(
                f"Connection {self.name} does not have associated secret. Please check the connection type"
            )

    def create_client(self, service_name: Optional[str] = None) -> BaseClient:
        """
        Returns a boto3 client initialized with the connection's credentials.

        Args:
            service_name (Optional[str]): The AWS service name of the boto3 client to initialize

        Returns:
            BaseClient: A boto3 client
        """
        default_aws_services_by_connection_type = {
            "ATHENA": "athena",
            "DYNAMODB": "dynamodb",
            "REDSHIFT": "redshift",
            "S3": "s3",
            "S3_FOLDER": "s3",
        }
        if not service_name and self.type not in default_aws_services_by_connection_type:
            raise RuntimeError("Please specify a service name to initialize a client")
        return self._get_aws_client_with_connection_credentials(
            service_name or default_aws_services_by_connection_type[self.type],
            self._connection_creds,
            self._secrets_manager_api,
        )

    def _find_secret_arn(self) -> str:
        """
        Finds the ARN of the secret associated with the connection. Looks in
        connection.data.glueProperties for DNA connections and DataZone GetConnection
        for redshift compute connections

        Returns:
                str: The ARN of the secret associated with the connection.

            Raises:
            RuntimeError: If there's an error finding the secret ARN.
        """
        secret_arn = None
        if len(self.physical_endpoints) >= 1 and self.physical_endpoints[0].glue_connection:  # type: ignore
            endpoint = self.physical_endpoints[0]  # type: ignore
            auth_config = getattr(endpoint.glue_connection, "authentication_configuration", None)
            if auth_config and isinstance(auth_config, dict):
                secret_arn = auth_config.get("secretArn")
        if not secret_arn:
            raise AttributeError(f"Connection {self.name} does not have associated secret")
        return secret_arn

    def _parse_secret(self, secret: Dict[str, Any]):
        """
        Parses the secret data and sets the appropriate attributes.

        Args:
            secret (Dict[str, Any]): The secret data.
        """
        if "SecretString" in secret:
            try:
                return json.loads(secret["SecretString"])
            except json.JSONDecodeError:
                return secret["SecretString"]
        else:
            return secret["SecretBinary"]

    def _map_connection_credentials_field(
        self, creds: Dict[str, Any]
    ) -> Optional[ConnectionCredentials]:
        if not creds:
            return None
        snake_case_creds = {self._camel_to_snake(key): value for key, value in creds.items()}
        if "expiration" in snake_case_creds and isinstance(snake_case_creds["expiration"], str):
            snake_case_creds["expiration"] = datetime.fromisoformat(snake_case_creds["expiration"])
        return ConnectionCredentials(**snake_case_creds)

    def _map_physical_endpoint_fields(self, endpoint: Dict[str, Any]) -> PhysicalEndpoint:
        snake_case_endpoint = {self._camel_to_snake(k): v for k, v in endpoint.items()}
        aws_location = endpoint.get("awsLocation", {})
        snake_case_endpoint["aws_region"] = aws_location.get("awsRegion", None)
        snake_case_endpoint["aws_account_id"] = aws_location.get("awsAccountId", None)
        snake_case_endpoint["access_role"] = aws_location.get("accessRole", None)
        snake_case_endpoint["iam_connection_id"] = aws_location.get("iamConnectionId", None)
        snake_case_endpoint.pop("aws_location", None)
        glue_connection_data = endpoint.get("glueConnection", {})
        if glue_connection_data:
            glue_connection_class = make_dataclass(
                "GlueConnection",
                [(self._camel_to_snake(k), type(v), None) for k, v in glue_connection_data.items()],
            )
            glue_connection_instance = glue_connection_class(
                **{self._camel_to_snake(k): v for k, v in glue_connection_data.items()}
            )
            snake_case_endpoint["_glue_connection"] = glue_connection_instance
            snake_case_endpoint.pop("glue_connection", None)
            snake_case_endpoint["host"] = glue_connection_data.get("connectionProperties", {}).get(
                "HOST", None
            )
            snake_case_endpoint["port"] = glue_connection_data.get("connectionProperties", {}).get(
                "PORT", None
            )

        snake_case_endpoint["_connection_instance"] = self
        return PhysicalEndpoint(**snake_case_endpoint)

    def _create_connection_data(self, connection_data: Dict[str, Any]):
        fields = []
        connection_data_snake_case = {}
        # Flattening fields (only 1 level deep) to be accessed using connection.data.<field_name>
        for key, value in connection_data.items():
            if key == "props" and isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    # This level contains PropertiesOutput key
                    if isinstance(sub_value, dict):
                        for prop_key, prop_value in sub_value.items():
                            if isinstance(prop_value, (dict, list)):
                                fields.append(
                                    (
                                        self._camel_to_snake(prop_key),
                                        type(prop_value),
                                        field(default_factory=self._default_factory(prop_value)),
                                    )
                                )
                            else:
                                fields.append(
                                    (
                                        self._camel_to_snake(prop_key),
                                        type(prop_value),
                                        field(default=prop_value),
                                    )
                                )
                            connection_data_snake_case[self._camel_to_snake(prop_key)] = prop_value
                    else:
                        fields.append(
                            (
                                self._camel_to_snake(sub_key),
                                type(sub_value),
                                field(default=sub_value),
                            )
                        )
                        connection_data_snake_case[self._camel_to_snake(sub_key)] = sub_value
            elif key != "ResponseMetadata" and key != "connectionCredentials":
                if isinstance(value, (dict, list)):
                    fields.append(
                        (
                            self._camel_to_snake(key),
                            type(value),
                            field(default_factory=self._default_factory(value)),
                        )
                    )
                else:
                    fields.append((self._camel_to_snake(key), type(value), field(default=value)))
                connection_data_snake_case[self._camel_to_snake(key)] = value
        connection_data_class_name = "ConnectionData"
        connection_data_class = make_dataclass(connection_data_class_name, fields)

        def custom_repr_for_credentials_password(self):
            data_dict = asdict(self)
            if data_dict.get("credentials") and "usernamePassword" in data_dict["credentials"]:
                data_dict["credentials"]["usernamePassword"]["password"] = "****"
            data_fields = []
            for data_key, data_value in data_dict.items():
                if (isinstance(data_value, list) or isinstance(data_value, dict)) and len(
                    data_value
                ) > 0:
                    data_fields.append(f"{data_key}={data_value}")
                elif isinstance(data_value, str):
                    data_fields.append(f"{data_key}='{data_value}'")
            return f"{connection_data_class_name}({','.join(data_fields)})"

        setattr(connection_data_class, "__repr__", custom_repr_for_credentials_password)
        return connection_data_class(**connection_data_snake_case)

    def _default_factory(self, value):
        return lambda: value

    def _camel_to_snake(self, name: str) -> str:
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _get_aws_client_with_connection_credentials(
        self,
        service_name: str,
        connection_credentials: Optional[ConnectionCredentials],
        existing_client: BaseClient,
    ) -> BaseClient:
        if connection_credentials is None:
            self._invoke_get_connection_and_populate_fields()
            connection_credentials = self._connection_creds
        if connection_credentials is not None:
            if any(
                getattr(connection_credentials, key) is None
                for key in ["access_key_id", "secret_access_key", "session_token"]
            ):
                raise RuntimeError("Unable to find credentials for project.iam connection")
            region_name = existing_client.meta.region_name
            project_override_config = self._project_config.overrides.get(service_name, {})
            override_region_name = project_override_config.get("region")
            if not override_region_name and self.physical_endpoints:
                region_name = self.physical_endpoints[0].aws_region or region_name
            override_endpoint_url = project_override_config.get("endpoint_url")
            return boto3.client(  # type: ignore
                service_name=service_name,
                region_name=region_name,
                endpoint_url=override_endpoint_url,
                aws_access_key_id=connection_credentials.access_key_id,
                aws_secret_access_key=connection_credentials.secret_access_key,
                aws_session_token=connection_credentials.session_token,
            )
        raise RuntimeError(f"Encountered an error getting AWS client for {service_name}.")

    def _catalog_builder(self, get_catalog_response) -> Catalog:
        catalog_name = get_catalog_response.get("Name")
        parent_catalog_names = "_".join(get_catalog_response.get("ParentCatalogNames", []))
        spark_catalog_name = (
            f"{parent_catalog_names}_{catalog_name}" if parent_catalog_names else catalog_name
        )
        return Catalog(
            name=catalog_name,
            id=get_catalog_response.get("CatalogId"),
            type=get_catalog_response.get("CatalogType"),
            spark_catalog_name=spark_catalog_name,
            resource_arn=get_catalog_response.get("ResourceArn"),
            domain_id=str(self.domain_id),
            project_id=str(self.project_id),
            glue_api=self._glue_api,
        )

    def _validate_catalog_id(self, catalog_id: str) -> bool:
        """
        A catalog name starts with an aws account followed by a catalog name

        Args:
            catalog_id (str): The identifier of the catalog. Defaults to the connections default Catalog identified by its accountId

        Returns:
            bool
        """
        pattern = r"^\d{12}:.+"
        return bool(re.match(pattern, catalog_id))

    def _validate_aws_account_id(self, id: str) -> bool:
        """
        A catalog name starts with an aws account followed by a catalog name

        Args:
            catalog_id (str): The identifier of the catalog. Defaults to the connections default Catalog identified by its accountId

        Returns:
            bool
        """
        pattern = r"^\d{12}"
        return bool(re.match(pattern, id))

    def _invoke_get_connection_and_populate_fields(self):
        try:
            connection_response = self._datazone_api.get_connection(  # type: ignore
                domainIdentifier=self.domain_id,
                identifier=self.id,
                withSecret=True,
            )
            connection_credentials = connection_response.get("connectionCredentials", None)
            self._connection_creds = self._map_connection_credentials_field(connection_credentials)

            self.physical_endpoints = [
                self._map_physical_endpoint_fields(endpoint)
                for endpoint in connection_response.get("physicalEndpoints", [])
            ]

            self.__connection_data = connection_response
        except ClientError as e:
            logging.warning(
                f"Encountered an error getting connection details for connection {self.name}: {AWSClientException(e)}"
            )
