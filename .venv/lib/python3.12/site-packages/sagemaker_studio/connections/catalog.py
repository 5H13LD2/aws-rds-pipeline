from dataclasses import dataclass, field
from typing import List

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from sagemaker_studio.exceptions import AWSClientException

from ..database.database import Database


@dataclass
class Catalog:
    """
    Represents a catalog within SageMaker Unified Studio. A catalog is a container for
    databases and provides methods to retrieve information about those
    databases.

    Attributes:
        name (str): The name of the catalog.
        id (str): The unique identifier of the catalog.
        type (str): The type of the catalog.
        spark_catalog_name (str): The catalog name used for Spark, including its parent catalogs
        resource_arn (str): The Amazon Resource Name (ARN) of the catalog.
        domain_id (str): The ID of the domain this catalog belongs to.
        project_id (str): The ID of the project this catalog belongs to.
    """

    name: str = field()
    id: str = field()
    type: str = field()
    resource_arn: str = field()

    def __init__(
        self,
        name: str,
        id: str,
        type: str,
        spark_catalog_name: str,
        resource_arn: str,
        domain_id: str,
        project_id: str,
        glue_api: BaseClient,
    ):
        """
        Initializes a new Catalog instance.

        Args:
            name (str): The name of the catalog.
            id (str): The unique identifier of the catalog.
            type (str): The type of the catalog.
            spark_catalog_name (str): The catalog name used for Spark, including its parent catalogs
            resource_arn (str): The Amazon Resource Name (ARN) of the catalog.
            domain_id (str): The ID of the domain this catalog belongs to.
            project_id (str): The ID of the project this catalog belongs to.
            glue_api (BaseClient): The Glue API client used for database operations.
        """
        self.name = name
        self.id = id
        self.type = type
        self.spark_catalog_name = spark_catalog_name
        self.resource_arn = resource_arn
        self.domain_id = domain_id
        self.project_id = project_id
        self._glue_api = glue_api

    def database(self, name: str) -> Database:
        """
        Retrieves a specific database in the catalog given its name.

        Args:
            name (str): The name of the database.

        Returns:
            Database: The Database object.
        """
        try:
            get_database_response = self._glue_api.get_database(Name=name, CatalogId=self.id).get("Database")  # type: ignore
            return Database(
                name=get_database_response.get("Name"),
                catalog_id=get_database_response.get("CatalogId"),
                location_uri=get_database_response.get("LocationUri"),
                domain_id=self.domain_id,
                project_id=self.project_id,
                glue_api=self._glue_api,
            )
        except ClientError as e:
            raise AttributeError(f"Unable to access database '{name}`: {AWSClientException(e)}'")
        except Exception as e:
            raise RuntimeError(f"Encountered an error getting database '{name}'", e)

    @property
    def databases(self) -> List[Database]:
        """
        Retrieves a list of all databases in the catalog.

        Returns:
            List[Database]: A list of Database objects.
        """
        try:
            databases_paginator = self._glue_api.get_paginator("get_databases")
            databases_page_iterator = databases_paginator.paginate(CatalogId=self.id)
            connection_databases: List[Database] = []
            for page in databases_page_iterator:
                for database in page.get("DatabaseList", []):
                    connection_databases.append(
                        Database(
                            name=database.get("Name"),
                            catalog_id=database.get("CatalogId"),
                            location_uri=database.get("LocationUri"),
                            domain_id=self.domain_id,
                            project_id=self.project_id,
                            glue_api=self._glue_api,
                        )
                    )
            return connection_databases
        except ClientError as e:
            raise RuntimeError("Encountered an error listing databases", AWSClientException(e))
