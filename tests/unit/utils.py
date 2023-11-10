from typing import Optional
import boto3

from .constants import AWS_REGION, BUCKET_NAME, CATALOG_ID, DATABASE_NAME


class MockAWSService:
    def create_database(self, name: str = DATABASE_NAME, catalog_id: str = CATALOG_ID):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": name}, CatalogId=catalog_id)

    def create_table(
            self,
            table_name: str,
            database_name: str = DATABASE_NAME,
            catalog_id: str = CATALOG_ID,
            location: Optional[str] = "auto",
    ):
        glue = boto3.client("glue", region_name=AWS_REGION)
        if location == "auto":
            location = f"s3://{BUCKET_NAME}/tables/{table_name}"
        glue.create_table(
            CatalogId=catalog_id,
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                    ],
                    "Location": location,
                },
                "PartitionKeys": [
                    {
                        "Name": "dt",
                        "Type": "date",
                    },
                ],
                "TableType": "table",
                "Parameters": {
                    "compressionType": "snappy",
                    "classification": "parquet",
                    "projection.enabled": "false",
                    "typeOfData": "file",
                },
            },
        )

    def create_iceberg_table(
            self,
            table_name: str,
            database_name: str = DATABASE_NAME,
            catalog_id: str = CATALOG_ID):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            CatalogId=catalog_id,
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": f"s3://{BUCKET_NAME}/tables/data/{table_name}",
                },
                "PartitionKeys": [
                    {
                        "Name": "dt",
                        "Type": "date",
                    },
                ],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "metadata_location": f"s3://{BUCKET_NAME}/tables/metadata/{table_name}/123.json",
                    "table_type": "iceberg",
                },
            },
        )
