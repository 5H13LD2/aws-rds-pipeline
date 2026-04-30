import unittest
from typing import Any, Dict, List
from unittest.mock import Mock

from botocore.exceptions import ClientError

from sagemaker_studio import Catalog, Database, Table

from ..utils import _create_mock_paginator

GET_DATABASES_PAGINATED_RESPONSE: List[Dict] = [
    {
        "DatabaseList": [
            {
                "Name": "bogus_database_00",
                "Description": "Created by DataZone for project bogus_project_00",
                "LocationUri": "s3://bogus_bucket/bogus_domain_00/bogus_project_00/db0",
                "CatalogId": "0123456789",
            },
            {
                "Name": "bogus_database_01",
                "Description": "Created by DataZone for project bogus_project_00",
                "LocationUri": "s3://bogus_bucket/bogus_domain_00/bogus_project_00/db1",
                "CatalogId": "0123456789",
            },
        ]
    },
    {
        "DatabaseList": [
            {
                "Name": "bogus_database_02",
                "Description": "Created by DataZone for project bogus_project_00",
                "LocationUri": "s3://bogus_bucket/bogus_domain_00/bogus_project_00/db2",
                "CatalogId": "0123456789",
            },
        ]
    },
]

GET_DATABASE_RESPONSE: Dict[str, Any] = {
    "Database": {
        "Name": "bogus_database_00",
        "Description": "Created by DataZone for project bogus_project_00",
        "LocationUri": "s3://bogus_bucket/bogus_domain/bogus_project/db0",
        "CatalogId": "0123456789",
    },
}

GET_TABLES_PAGINATED_RESPONSE: List[Dict] = [
    {
        "TableList": [
            {
                "Name": "bogus_table_00",
                "DatabaseName": "bogus_database_00",
                "CatalogId": "0123456789",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "col0", "Type": "string"},
                        {"Name": "col1", "Type": "date"},
                        {"Name": "col2", "Type": "bigint"},
                    ],
                    "Location": "s3://bogus_bucket/bogus_domain/bogus_project/db0",
                },
            },
            {
                "Name": "bogus_table_01",
                "DatabaseName": "bogus_database_00",
                "CatalogId": "0123456789",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "col0", "Type": "date"},
                    ],
                    "Location": "s3://bogus_bucket/bogus_domain/bogus_project/db0",
                },
            },
        ]
    },
    {
        "TableList": [
            {
                "Name": "bogus_table_03",
                "DatabaseName": "bogus_database_00",
                "CatalogId": "0123456789",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "col0", "Type": "bigint"},
                    ],
                    "Location": "s3://bogus_bucket/bogus_domain/bogus_project/db0",
                },
            },
        ]
    },
]

GET_TABLE_RESPONSE: Dict[str, Any] = {
    "Table": {
        "Name": "bogus_table_00",
        "DatabaseName": "bogus_database_00",
        "CatalogId": "0123456789",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "col0", "Type": "string"},
                {"Name": "col1", "Type": "date"},
                {"Name": "col2", "Type": "bigint"},
            ],
            "Location": "s3://bogus_bucket/bogus_domain/bogus_project/db0",
        },
    },
}


class TestCatalog(unittest.TestCase):

    def test_get_catalog_database(self):
        test_catalog = self.get_test_catalog()
        test_catalog._glue_api.get_database.return_value = GET_DATABASE_RESPONSE

        catalog_db: Database = test_catalog.database("bogus_database_00")

        assert catalog_db.name == GET_DATABASE_RESPONSE["Database"]["Name"]
        assert catalog_db.catalog_id == GET_DATABASE_RESPONSE["Database"]["CatalogId"]
        assert catalog_db.location_uri == GET_DATABASE_RESPONSE["Database"]["LocationUri"]
        assert catalog_db.domain_id == "dzd_1234"
        assert catalog_db.project_id == "abc123"

    def test_get_catalog_databases(self):
        test_catalog = self.get_test_catalog()
        get_databases_paginator = _create_mock_paginator(GET_DATABASES_PAGINATED_RESPONSE)
        test_catalog._glue_api.get_paginator = Mock()
        test_catalog._glue_api.get_paginator.side_effect = lambda x: get_databases_paginator

        catalog_dbs: List[Database] = test_catalog.databases

        assert len(catalog_dbs) == 3
        assert any(db.name == "bogus_database_00" for db in catalog_dbs)
        assert any(db.name == "bogus_database_01" for db in catalog_dbs)
        assert any(db.name == "bogus_database_02" for db in catalog_dbs)
        assert all(db.catalog_id == "0123456789" for db in catalog_dbs)
        assert all(db.domain_id == "dzd_1234" for db in catalog_dbs)
        assert all(db.project_id == "abc123" for db in catalog_dbs)

    def test_get_catalog_database_table(self):
        test_catalog = self.get_test_catalog()
        test_catalog._glue_api.get_database.return_value = GET_DATABASE_RESPONSE
        test_catalog._glue_api.get_table.return_value = GET_TABLE_RESPONSE

        catalog_db: Database = test_catalog.database("bogus_database_00")
        catalog_db_table: Table = catalog_db.table("bogus_table_00")

        assert catalog_db_table.name == GET_TABLE_RESPONSE["Table"]["Name"]
        assert catalog_db_table.database_name == GET_TABLE_RESPONSE["Table"]["DatabaseName"]
        assert catalog_db_table.catalog_id == GET_TABLE_RESPONSE["Table"]["CatalogId"]
        assert (
            catalog_db_table.location
            == GET_TABLE_RESPONSE["Table"]["StorageDescriptor"]["Location"]
        )
        assert (catalog_db_table.columns[0].name, catalog_db_table.columns[0].type) == (
            "col0",
            "string",
        )
        assert (catalog_db_table.columns[1].name, catalog_db_table.columns[1].type) == (
            "col1",
            "date",
        )
        assert (catalog_db_table.columns[2].name, catalog_db_table.columns[2].type) == (
            "col2",
            "bigint",
        )

    def test_get_catalog_database_tables(self):
        test_catalog = self.get_test_catalog()
        test_catalog._glue_api.get_database.return_value = GET_DATABASE_RESPONSE
        get_tables_paginator = _create_mock_paginator(GET_TABLES_PAGINATED_RESPONSE)
        test_catalog._glue_api.get_paginator = Mock()
        test_catalog._glue_api.get_paginator.side_effect = lambda x: get_tables_paginator

        catalog_db: Database = test_catalog.database("bogus_database_00")
        catalog_db_tables: List[Table] = catalog_db.tables

        assert len(catalog_db_tables) == 3
        assert any(table.name == "bogus_table_00" for table in catalog_db_tables)
        assert any(table.name == "bogus_table_01" for table in catalog_db_tables)
        assert any(table.name == "bogus_table_03" for table in catalog_db_tables)
        assert all(
            table.location == "s3://bogus_bucket/bogus_domain/bogus_project/db0"
            for table in catalog_db_tables
        )
        assert all(table.catalog_id == "0123456789" for table in catalog_db_tables)
        assert any(table.database_name == "bogus_database_00" for table in catalog_db_tables)
        assert all(table.columns[0].name == "col0" for table in catalog_db_tables)
        assert len(catalog_db_tables[0].columns) == 3
        assert len(catalog_db_tables[1].columns) == 1
        assert len(catalog_db_tables[2].columns) == 1

    def get_test_catalog(self):
        return Catalog(
            name="test_catalog",
            id="0123456789",
            type="test_type",
            spark_catalog_name="parent_catalog-test_catalog",
            resource_arn="glue:catalog:1234",
            domain_id="dzd_1234",
            project_id="abc123",
            glue_api=Mock(),
        )

    def test_get_catalog_databases_paginator_throws_client_error(self):
        test_catalog = self.get_test_catalog()
        test_catalog._glue_api.get_paginator = Mock()
        test_catalog._glue_api.get_paginator.side_effect = ClientError(
            error_response={"Error": {"Code": "ThrottlingException"}},
            operation_name="GetDatabases",
        )
        with self.assertRaises(RuntimeError) as context:
            test_catalog.databases
            self.assertTrue("Encountered an error listing databases" in context.exception)

    def test_get_catalog_database_tables_throws_client_error(self):
        test_catalog = self.get_test_catalog()
        test_catalog._glue_api.get_database.return_value = GET_DATABASE_RESPONSE
        test_catalog._glue_api.get_paginator = Mock()
        test_catalog._glue_api.get_paginator.side_effect = ClientError(
            error_response={"Error": {"Code": "ThrottlingException"}},
            operation_name="GetTables",
        )

        catalog_db: Database = test_catalog.database("bogus_database_00")
        with self.assertRaises(RuntimeError) as context:
            catalog_db.tables
            self.assertTrue("Encountered an error getting tables for database" in context.exception)
