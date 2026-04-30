import unittest
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError

from sagemaker_studio import ClientConfig
from sagemaker_studio.connections import Connection
from sagemaker_studio.connections.catalog import Catalog
from sagemaker_studio.connections.connection import ConnectionCredentials, PhysicalEndpoint

GET_CATALOGS_RESPONSE: Dict[str, Any] = {
    "CatalogList": [
        {
            "CatalogId": "1234567890:9876543210",
            "Name": "bogus_catalog_00",
            "ResourceArn": "arn:aws:glue:us-east-1:1234567890:catalog/bogus_catalog_00",
            "CatalogType": "bogus_catalog_type_00",
            "ParentCatalogNames": ["bogus_parent_00", "bogus_parent_01"],
        },
        {
            "CatalogId": "9876543210:1234567890",
            "Name": "bogus_catalog_01",
            "ResourceArn": "arn:aws:glue:us-east-1:1234567890:catalog/bogus_catalog_01",
            "CatalogType": "bogus_catalog_type_00",
            "ParentCatalogNames": ["bogus_parent_00", "bogus_parent_01"],
        },
    ]
}

GET_CATALOG_RESPONSE: Dict[str, Any] = {
    "Catalog": {
        "CatalogId": "1234567890:9876543210",
        "Name": "bogus_catalog_00",
        "ResourceArn": "arn:aws:glue:us-east-1:1234567890:catalog/bogus_catalog_00",
        "CatalogType": "bogus_catalog_type_00",
        "ParentCatalogNames": ["bogus_parent_00", "bogus_parent_01"],
    }
}

BOGUS_CONNECTION_CREDENTIALS = {
    "AccessKeyId": "access_key_id",
    "SecretAccessKey": "secret_access_key",
    "SessionToken": "session_token",
    "expiration": datetime.today().isoformat(),
}

DZ_API_MOCK = Mock()
DZ_API_MOCK.get_connection.return_value = {"connectionCredentials": BOGUS_CONNECTION_CREDENTIALS}

SECRETS_MANAGER_MOCK = Mock()
SECRETS_MANAGER_MOCK.meta = Mock()
SECRETS_MANAGER_MOCK.meta.mock_add_spec(
    boto3.session.Session(region_name="any-region-1").client("secretsmanager").meta, spec_set=True
)
SECRETS_MANAGER_MOCK.meta.endpoint_url = "https://example.com"
SECRETS_MANAGER_MOCK.meta.region_name = "us-east-1"

GLUE_MOCK = Mock()
GLUE_MOCK.meta = Mock()
GLUE_MOCK.meta.mock_add_spec(
    boto3.session.Session(region_name="any-region-1").client("glue").meta, spec_set=True
)
GLUE_MOCK.meta.endpoint_url = "https://example.com"
GLUE_MOCK.meta.region_name = "us-east-1"


class TestConnection(unittest.TestCase):
    def test_map_connection_credentials_field_with_data(self):
        creds = {
            "accessKeyId": "ASIAVRUVTZK37EXAMPLE",
            "secretAccessKey": "G4cPFS/qWP7d0GCkmIiiTnqTwhv0uSxsEXAMPLE",
            "sessionToken": "IQoJb3JpZ2luX2VjEBwaCXVzLEXAMPLE...",
            "expiration": "2024-10-31T20:15:55",
        }
        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        result = connection._map_connection_credentials_field(creds)
        assert result is not None
        self.assertEqual(result.access_key_id, "ASIAVRUVTZK37EXAMPLE")
        self.assertEqual(result.secret_access_key, "G4cPFS/qWP7d0GCkmIiiTnqTwhv0uSxsEXAMPLE")
        self.assertEqual(result.session_token, "IQoJb3JpZ2luX2VjEBwaCXVzLEXAMPLE...")
        self.assertEqual(result.expiration, datetime.fromisoformat("2024-10-31T20:15:55"))

    def test_empty_physical_endpoints(self):
        connection_data = {
            "ConnectionId": "12345",
            "ConnectionType": "Spark",
            "PhysicalEndpoints": [],
        }
        connection = Connection(
            connection_data, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig()
        )
        self.assertFalse(hasattr(connection, "endpoint_uri"))
        self.assertFalse(hasattr(connection, "port"))

    def test_camel_to_snake_case(self):
        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        self.assertEqual(connection._camel_to_snake("CamelCaseTest"), "camel_case_test")
        self.assertEqual(connection._camel_to_snake("simpleTest"), "simple_test")
        self.assertEqual(connection._camel_to_snake("testCamel2Case"), "test_camel2_case")

    def test_get_aws_client_with_connection_credentials_invalid_credentials(self):
        conn_credentials = ConnectionCredentials(access_key_id="123")
        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        with self.assertRaises(RuntimeError) as context:
            connection._get_aws_client_with_connection_credentials("glue", conn_credentials, Mock())
            self.assertTrue(
                "Unable to find credentials for project.iam connection" in context.exception
            )

    def test_get_connection_catalog_named_catalog_without_account_id_prefix(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "physicalEndpoints": [
                {
                    "awsLocation": {
                        "accessRole": "test-access-role",
                        "awsAccountId": "123456890-Account-Id",
                    },
                    "glueConnectionName": "glue-abc-conn",
                }
            ],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {"connectionId": "12345", "type": "IAM"},
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalog.return_value = GET_CATALOG_RESPONSE

        connection_catalog: Catalog = connection.catalog("bogus_catalog_00")

        assert connection_catalog.name == GET_CATALOG_RESPONSE["Catalog"]["Name"]
        assert connection_catalog.type == GET_CATALOG_RESPONSE["Catalog"]["CatalogType"]
        assert connection_catalog.resource_arn == GET_CATALOG_RESPONSE["Catalog"]["ResourceArn"]
        assert connection_catalog.id == GET_CATALOG_RESPONSE["Catalog"]["CatalogId"]
        assert (
            connection_catalog.spark_catalog_name
            == f"{'_'.join(GET_CATALOG_RESPONSE['Catalog']['ParentCatalogNames'])}_{GET_CATALOG_RESPONSE['Catalog']['Name']}"
        )
        connection._glue_api.get_catalog.assert_called_once_with(
            CatalogId="123456890-Account-Id:bogus_catalog_00"
        )

    def test_get_connection_catalog_default_catalog(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "physicalEndpoints": [
                {
                    "awsLocation": {
                        "accessRole": "test-access-role",
                        "awsAccountId": "123456890-Account-Id",
                    },
                    "glueConnectionName": "glue-abc-conn",
                }
            ],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {
                "connectionId": "12345",
                "type": "IAM",
                "physicalEndpoints": [
                    {
                        "awsLocation": {
                            "accessRole": "test-access-role",
                            "awsAccountId": "123456890-Account-Id",
                        },
                        "glueConnectionName": "glue-abc-conn",
                    }
                ],
            },
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalog.return_value = GET_CATALOG_RESPONSE

        connection_catalog: Catalog = connection.catalog()

        assert connection_catalog.name == GET_CATALOG_RESPONSE["Catalog"]["Name"]
        assert connection_catalog.type == GET_CATALOG_RESPONSE["Catalog"]["CatalogType"]
        assert connection_catalog.resource_arn == GET_CATALOG_RESPONSE["Catalog"]["ResourceArn"]
        assert connection_catalog.id == GET_CATALOG_RESPONSE["Catalog"]["CatalogId"]
        assert (
            connection_catalog.spark_catalog_name
            == f"{'_'.join(GET_CATALOG_RESPONSE['Catalog']['ParentCatalogNames'])}_{GET_CATALOG_RESPONSE['Catalog']['Name']}"
        )
        connection._glue_api.get_catalog.assert_called_with(CatalogId="123456890-Account-Id")

    def test_get_connection_catalog_catalog_id_is_an_account_id(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "physicalEndpoints": [
                {
                    "awsLocation": {
                        "accessRole": "test-access-role",
                        "awsAccountId": "123456890-Account-Id",
                    },
                    "glueConnectionName": "glue-abc-conn",
                }
            ],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {
                "connectionId": "12345",
                "type": "IAM",
                "physicalEndpoints": [
                    {
                        "awsLocation": {
                            "accessRole": "test-access-role",
                            "awsAccountId": "123456890-Account-Id",
                        },
                        "glueConnectionName": "glue-abc-conn",
                    }
                ],
            },
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalog.return_value = GET_CATALOG_RESPONSE

        connection_catalog: Catalog = connection.catalog("123456789012")

        assert connection_catalog.name == GET_CATALOG_RESPONSE["Catalog"]["Name"]
        assert connection_catalog.type == GET_CATALOG_RESPONSE["Catalog"]["CatalogType"]
        assert connection_catalog.resource_arn == GET_CATALOG_RESPONSE["Catalog"]["ResourceArn"]
        assert connection_catalog.id == GET_CATALOG_RESPONSE["Catalog"]["CatalogId"]
        connection._glue_api.get_catalog.assert_called_with(CatalogId="123456789012")

    def test_get_connection_catalog_default_catalog_no_physical_endpoint_throws_error(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {
                "connectionId": "12345",
                "type": "IAM",
            },
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )

        with self.assertRaises(RuntimeError) as context:
            connection.catalog()
            self.assertTrue("No physical endpoints found for the connection" in context.exception)

    def test_get_connection_catalog_throws_client_error(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "physicalEndpoints": [
                {
                    "awsLocation": {
                        "accessRole": "test-access-role",
                        "awsAccountId": "123456890-Account-Id",
                    },
                    "glueConnectionName": "glue-abc-conn",
                }
            ],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {
                "connectionId": "12345",
                "type": "IAM",
            },
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalog.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDeniedException"}}, operation_name="GetCatalog"
        )
        with self.assertRaises(AttributeError) as context:
            connection.catalog("catalog_1")
            self.assertTrue("Unable to access catalog" in context.exception)

    def test_get_connection_catalogs(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {"connectionId": "12345", "type": "IAM"},
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalogs.return_value = GET_CATALOGS_RESPONSE

        connection_catalogs: List[Catalog] = connection.catalogs

        assert len(connection_catalogs) == 2
        assert any(catalog.name == "bogus_catalog_00" for catalog in connection_catalogs)
        assert any(catalog.name == "bogus_catalog_01" for catalog in connection_catalogs)
        assert all(catalog.type == "bogus_catalog_type_00" for catalog in connection_catalogs)
        assert all(
            "bogus_parent_00_bogus_parent_01_" in catalog.spark_catalog_name
            for catalog in connection_catalogs
        )
        connection._glue_api.get_catalogs.assert_called_with(Recursive=True, HasDatabases=True)

    def test_get_connection_catalogs_throws_client_error(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {"connectionId": "12345", "type": "IAM"},
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalogs.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDeniedException"}},
            operation_name="GetCatalogs",
        )

        with self.assertRaises(RuntimeError) as context:
            connection.catalogs
            self.assertTrue(
                "Encountered an error getting catalogs for the connection" in context.exception
            )

    def test_get_catalogs_non_iam_non_lakehouse_connection(self):
        connection = Connection(
            {"connectionId": "12345", "type": "REDSHIFT"},
            GLUE_MOCK,
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        with self.assertRaises(AttributeError) as context:
            connection.catalogs
            self.assertTrue(
                "Catalogs are only available for IAM or LAKEHOUSE connections" in context.exception
            )

    def test_get_catalog_non_iam_non_lakehouse_connection(self):
        connection = Connection(
            {"connectionId": "12345", "type": "REDSHIFT"},
            GLUE_MOCK,
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        with self.assertRaises(AttributeError) as context:
            connection.catalog
            self.assertTrue(
                "Catalogs are only available for IAM or LAKEHOUSE connections" in context.exception
            )

    def test_get_connection_catalogs_throws_other_error(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {"connectionId": "12345", "type": "IAM"},
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalogs.side_effect = AttributeError("Some other error")

        with self.assertRaises(RuntimeError) as context:
            connection.catalogs
            self.assertTrue(
                "Encountered an unexpected exception getting catalogs for the connection"
                in context.exception
            )

    def test_print_connection(self):
        conn_data = {
            "name": "test_conn",
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = conn_data
        connection = Connection(
            conn_data, GLUE_MOCK, datazone_api_mock, SECRETS_MANAGER_MOCK, ClientConfig()
        )
        assert "None" not in str(connection)

    def test_print_physical_endpoint(self):
        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        physical_endpoint = PhysicalEndpoint(
            _connection_instance=connection, host="bogus_host", port=1234
        )
        assert "None" not in str(physical_endpoint)

    def test_parse_secret_using_valid_json(self):
        secret_1 = {"SecretString": '{"username":"user","password": "password"}'}

        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        self.assertEqual(
            connection._parse_secret(secret_1), {"username": "user", "password": "password"}
        )

        secret_2 = {"SecretString": '{"username":"user","password": "password"}'}
        self.assertEqual(
            connection._parse_secret(secret_2), {"username": "user", "password": "password"}
        )

    def test_parse_secret_using_non_json_secret(self):
        secret_1 = {"SecretString": "ThisIsASecret"}

        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        self.assertEqual(connection._parse_secret(secret_1), "ThisIsASecret")

    def test_parse_secret_using_secret_binary(self):
        secret_1 = {"SecretBinary": "VGhpc0lzQVNlY3JldA=="}

        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        self.assertEqual(connection._parse_secret(secret_1), "VGhpc0lzQVNlY3JldA==")

    def test_find_secret_arn_from_glue_properties_in_physical_endpoints(self):
        dz_api_mock = deepcopy(DZ_API_MOCK)
        dz_api_mock.get_connection.return_value = {
            "connectionCredentials": {
                "AccessKeyId": "access_key_id",
                "SecretAccessKey": "secret_access_key",
                "SessionToken": "session_token",
                "expiration": datetime.today().isoformat(),
            },
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                    "glueConnection": {
                        "name": "this-connection-does-not-exist",
                        "connectionType": "SNOWFLAKE",
                        "status": "READY",
                        "authenticationConfiguration": {
                            "authenticationType": "BASIC",
                            "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                        },
                    },
                }
            ],
        }
        connection = Connection(
            {
                "physicalEndpoints": [
                    {
                        "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                        "glueConnectionName": "this-connection-does-not-exist",
                        "glueConnection": {
                            "name": "this-connection-does-not-exist",
                            "connectionType": "SNOWFLAKE",
                            "status": "READY",
                            "authenticationConfiguration": {
                                "authenticationType": "BASIC",
                                "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                            },
                        },
                    }
                ]
            },
            Mock(),
            dz_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )

        secret_arn = connection._find_secret_arn()
        self.assertEqual(
            secret_arn, "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234"
        )

    def test_find_secret_arn_from_glue_properties_in_physical_endpoints_not_found(self):
        connection_1 = Connection(
            {
                "physicalEndpoints": [
                    {
                        "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                        "glueConnectionName": "this-connection-does-not-exist",
                        "glueConnection": {
                            "name": "this-connection-does-not-exist",
                            "connectionType": "SNOWFLAKE",
                            "status": "READY",
                            "authenticationConfiguration": {"authenticationType": "BASIC"},
                        },
                    }
                ]
            },
            Mock(),
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )

        with self.assertRaises(AttributeError) as context:
            connection_1._find_secret_arn()
            self.assertTrue("does not have associated secret" in context.exception)

        connection_2 = Connection(
            {
                "physicalEndpoints": [
                    {
                        "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                        "glueConnectionName": "this-connection-does-not-exist",
                        "glueConnection": {
                            "name": "this-connection-does-not-exist",
                            "connectionType": "SNOWFLAKE",
                            "status": "READY",
                        },
                    }
                ]
            },
            Mock(),
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )

        with self.assertRaises(AttributeError) as context:
            connection_2._find_secret_arn()
            self.assertTrue("does not have associated secret" in context.exception)

        connection_3 = Connection(
            {
                "physicalEndpoints": [
                    {
                        "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                        "glueConnectionName": "this-connection-does-not-exist",
                    }
                ]
            },
            Mock(),
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )

        with self.assertRaises(AttributeError) as context:
            connection_3._find_secret_arn()
            self.assertTrue("does not have associated secret" in context.exception)

        connection_4 = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())

        with self.assertRaises(AttributeError) as context:
            connection_4._find_secret_arn()
            self.assertTrue("does not have associated secret" in context.exception)

    def test_get_secret_successful_response(self):
        secret_manager_mock = deepcopy(SECRETS_MANAGER_MOCK)
        dz_mock = deepcopy(DZ_API_MOCK)
        dz_mock.get_connection.return_value = {
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "123456789012", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                    "glueConnection": {
                        "name": "this-connection-does-not-exist",
                        "connectionType": "SNOWFLAKE",
                        "status": "READY",
                        "authenticationConfiguration": {
                            "authenticationType": "BASIC",
                            "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                        },
                    },
                }
            ],
        }
        secret_manager_mock.get_secret_value.return_value = {
            "SecretString": '{"username":"user","password": "password"}'
        }
        connection_data = {
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "123456789012", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                }
            ]
        }
        connection = Connection(
            connection_data, GLUE_MOCK, dz_mock, secret_manager_mock, ClientConfig()
        )
        with patch.object(connection, "_find_secret_arn") as mock:
            with patch.object(
                connection,
                "_get_aws_client_with_connection_credentials",
                return_value=secret_manager_mock,
            ):
                connection._secrets_manager_api = secret_manager_mock
                mock.return_value = "arn:aws:secretsmanager:us-east-1:secret"
                secret = connection.secret
                self.assertEqual(secret, {"username": "user", "password": "password"})

    def test_get_secret_client_error_from_secrets_manager(self):
        secret_manager_mock = deepcopy(SECRETS_MANAGER_MOCK)
        connection = Connection({}, GLUE_MOCK, DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        with patch.object(connection, "_find_secret_arn") as mock:
            mock.return_value = "arn:aws:secretsmanager:us-east-1:secret"
            with patch.object(
                connection,
                "_get_aws_client_with_connection_credentials",
                return_value=secret_manager_mock,
            ) as sm_api_mock:
                mock_client = sm_api_mock.return_value
                mock_client.get_secret_value.side_effect = ClientError(
                    error_response={"Error": {"Code": "AccessDeniedException"}},
                    operation_name="GetSecretValue",
                )
                with self.assertRaises(RuntimeError) as context:
                    connection.secret
                    self.assertTrue(
                        "Encountered an error getting secret for connection" in context.exception
                    )

    def test_simulated_e2e_get_secret_from_physical_endpoint(self):
        dz_api_mock = deepcopy(DZ_API_MOCK)
        dz_api_mock.get_connection.return_value = {
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                    "glueConnection": {
                        "name": "this-connection-does-not-exist",
                        "connectionType": "SNOWFLAKE",
                        "status": "READY",
                        "authenticationConfiguration": {
                            "authenticationType": "BASIC",
                            "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                        },
                    },
                }
            ],
        }
        connection_data = {
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                }
            ],
        }
        connection = Connection(
            connection_data,
            GLUE_MOCK,
            dz_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._secrets_manager_api = deepcopy(SECRETS_MANAGER_MOCK)
        connection._secrets_manager_api.get_secret_value.return_value = {
            "SecretString": '{"username":"user","password": "password"}'
        }
        with patch.object(
            connection,
            "_get_aws_client_with_connection_credentials",
            return_value=connection._secrets_manager_api,
        ):
            secret = connection.secret
            self.assertEqual(secret, {"username": "user", "password": "password"})
            connection._secrets_manager_api.get_secret_value.assert_called_once_with(
                SecretId="arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234"
            )

    def test_populated_physical_endpoints_with_glue_connection_property(self):
        dz_api_mock = deepcopy(DZ_API_MOCK)
        dz_api_mock.get_connection.return_value = {
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                    "glueConnection": {
                        "name": "this-connection-does-not-exist",
                        "connectionType": "SNOWFLAKE",
                        "status": "READY",
                        "authenticationConfiguration": {
                            "authenticationType": "BASIC",
                            "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                        },
                        "connectionProperties": {
                            "DATABASE": "TEST",
                            "HOST": "https://bogus_host.snowflakecomputing.com",
                            "PORT": "443",
                            "ROLE_ARN": "arn:aws:iam::1234567890:role/sagemaker_studio/sagemaker_studio_usr_role_bogus",
                            "SCHEMA": "PUBLIC",
                            "WAREHOUSE": "COMPUTE_WH",
                        },
                    },
                }
            ],
        }

        connection_data = {
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                }
            ],
        }
        connection = Connection(
            connection_data,
            GLUE_MOCK,
            dz_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        endpoint = connection.physical_endpoints[0]
        glue_connection = endpoint.glue_connection
        auth_config = glue_connection.authentication_configuration
        self.assertEqual(auth_config["authenticationType"], "BASIC")
        self.assertEqual(
            auth_config["secretArn"],
            "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
        )
        self.assertEqual(glue_connection.status, "READY")
        conn_props = glue_connection.connection_properties
        self.assertEqual(conn_props["HOST"], "https://bogus_host.snowflakecomputing.com")

    def test_populated_connection_with_data_password(self):
        connection = Connection(
            {
                "props": {
                    "redshiftProperties": {
                        "storage": {"workgroupName": "bogus-workgroup"},
                        "credentials": {
                            "usernamePassword": {
                                "password": "bogus_password",
                                "username": ("bogus_username"),
                            }
                        },
                        "jdbcIamUrl": (
                            "jdbc:redshift:iam://bogus_url"
                            "123456123456.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
                        ),
                        "jdbcUrl": (
                            "jdbc:redshift:iam://bogus_url"
                            "123456123456.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
                        ),
                        "databaseName": "dev",
                    }
                },
            },
            Mock(),
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        data = connection.data
        self.assertEqual(data.credentials.get("usernamePassword").get("password"), "bogus_password")
        data_repr = repr(data)
        self.assertIn("'password': '****'", data_repr)

    def test_get_connection_with_account_id_prefixed(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection = Connection(
            {"connectionId": "12345", "type": "IAM"},
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._glue_api = Mock()
        connection._glue_api.get_catalog.return_value = GET_CATALOG_RESPONSE

        connection_catalog: Catalog = connection.catalog("123456789123:bogus_catalog_00")

        assert connection_catalog.name == GET_CATALOG_RESPONSE["Catalog"]["Name"]
        assert connection_catalog.type == GET_CATALOG_RESPONSE["Catalog"]["CatalogType"]
        assert connection_catalog.resource_arn == GET_CATALOG_RESPONSE["Catalog"]["ResourceArn"]
        assert connection_catalog.id == GET_CATALOG_RESPONSE["Catalog"]["CatalogId"]
        assert (
            connection_catalog.spark_catalog_name
            == f"{'_'.join(GET_CATALOG_RESPONSE['Catalog']['ParentCatalogNames'])}_{GET_CATALOG_RESPONSE['Catalog']['Name']}"
        )
        connection._glue_api.get_catalog.assert_called_once_with(
            CatalogId="123456789123:bogus_catalog_00"
        )

    def test_validate_catalog_id(self):
        cases = [
            ("123456789012:my-resource", True),
            ("123456789012:", False),
            ("12345678901:my-resource", False),
            ("abcdefghijkl:my-resource", False),
            ("123456789012:my:resource", True),
            ("123456789012", False),
            ("123456789012:my-resource/child-catalog", True),
            ("my-resource/child-catalog", False),
            ("my-resource", False),
            ("123:my-resource", False),
        ]
        connection = Connection({}, Mock(), DZ_API_MOCK, SECRETS_MANAGER_MOCK, ClientConfig())
        for case in cases:
            result = connection._validate_catalog_id(case[0])
            self.assertEqual(result, case[1])

    def test_invoke_get_connection_and_populate_fields_successful(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                    "glueConnection": {
                        "name": "bogus-connection-name",
                        "connectionType": "SNOWFLAKE",
                        "status": "READY",
                        "authenticationConfiguration": {
                            "authenticationType": "BASIC",
                            "secretArn": "arn:aws:secretsmanager:us-east-1:1234567890-mock:secret:mock-secret-1234",
                        },
                        "connectionProperties": {
                            "DATABASE": "TEST",
                            "HOST": "1234",
                            "PORT": "443",
                            "ROLE_ARN": "bogus_role",
                        },
                    },
                }
            ],
        }
        connection_data = {
            "physicalEndpoints": [
                {
                    "awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"},
                    "glueConnectionName": "this-connection-does-not-exist",
                }
            ],
        }
        connection = Connection(
            connection_data,
            GLUE_MOCK,
            datazone_api_mock,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        connection._invoke_get_connection_and_populate_fields()
        self.assertEqual(
            connection._connection_creds.access_key_id,
            BOGUS_CONNECTION_CREDENTIALS.get("AccessKeyId"),
        )
        self.assertEqual(
            connection.physical_endpoints[0].glue_connection.authentication_configuration[
                "authenticationType"
            ],
            "BASIC",
        )

    def test_connection_creds_property(self):
        connection = Connection(
            {"connectionId": "bogus_connection_id", "type": "IAM"},
            GLUE_MOCK,
            DZ_API_MOCK,
            SECRETS_MANAGER_MOCK,
            ClientConfig(),
        )
        self.assertEqual(connection.connection_creds.access_key_id, "access_key_id")
        self.assertEqual(connection.connection_creds.secret_access_key, "secret_access_key")
        self.assertEqual(connection.connection_creds.session_token, "session_token")

    def test_get_secret_successful_for_compute_redshift(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "props": {
                "redshiftProperties": {
                    "credentials": {
                        "usernamePassword": {
                            "password": "XXXXX_password",
                            "username": ("bogus_username"),
                        }
                    },
                }
            },
            "physicalEndpoints": [
                {"awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"}}
            ],
        }
        connection_data = {
            "physicalEndpoints": [
                {"awsLocation": {"awsAccountId": "1234567890", "awsRegion": "us-east-1"}}
            ],
        }
        connection = Connection(
            connection_data,
            Mock(),
            datazone_api_mock,
            Mock(),
            ClientConfig(),
        )
        secret = connection.secret
        self.assertEqual(secret, {"username": "bogus_username", "password": "XXXXX_password"})

    def test_get_aws_client_default(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "S3",
            "physicalEndpoints": [],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection_data = {
            "connectionId": "12345",
            "physicalEndpoints": [],
            "type": "S3",
        }
        connection = Connection(
            connection_data,
            Mock(),
            datazone_api_mock,
            boto3.client("secretsmanager", "us-east-1"),
            ClientConfig(),
        )

        s3_client = connection.create_client()
        self.assertEqual("s3", s3_client.meta.service_model.service_name)

    def test_get_aws_client_specific_service(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "S3",
            "physicalEndpoints": [],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection_data = {
            "connectionId": "12345",
            "physicalEndpoints": [],
            "type": "S3",
        }
        connection = Connection(
            connection_data,
            Mock(),
            datazone_api_mock,
            boto3.client("secretsmanager", "us-east-1"),
            ClientConfig(),
        )

        s3_client = connection.create_client("dynamodb")
        self.assertEqual("dynamodb", s3_client.meta.service_model.service_name)

    def test_get_aws_client_fails_for_connection_without_default_service(self):
        datazone_api_mock = Mock()
        datazone_api_mock.get_connection.return_value = {
            "connectionId": "12345",
            "type": "IAM",
            "physicalEndpoints": [],
            "connectionCredentials": BOGUS_CONNECTION_CREDENTIALS,
        }
        connection_data = {
            "connectionId": "12345",
            "physicalEndpoints": [],
            "type": "SNOWFLAKE",
        }
        connection = Connection(
            connection_data,
            Mock(),
            datazone_api_mock,
            boto3.client("secretsmanager", "us-east-1"),
            ClientConfig(),
        )

        with self.assertRaises(RuntimeError) as context:
            connection.create_client()
            self.assertTrue(
                "Please specify a service name to initialize a client" in context.exception
            )
