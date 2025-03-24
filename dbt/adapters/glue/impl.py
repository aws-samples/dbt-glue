import io
import json
import os
import re
import uuid
import boto3
from dbt.adapters.glue.util import get_columns_from_result, get_pandas_dataframe_from_result_file
from typing import Set, Dict, List, Any, Iterable, FrozenSet, Tuple

import agate
from concurrent.futures import Future

from dbt.adapters.base import available
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.glue import GlueConnectionManager
from dbt.adapters.glue.column import GlueColumn
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.relation import SparkRelation
from dbt.adapters.glue.lakeformation import (
    LfGrantsConfig,
    LfPermissions,
    LfTagsConfig,
    LfTagsManager,
)
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.exceptions import DbtDatabaseError, CompilationError
from dbt.adapters.base.impl import catch_as_completed
from dbt_common.utils import executor
from dbt_common.clients import agate_helper
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Glue")


class ColumnCsvMappingStrategy:
    _schema_mappings = {
        "timestamp": "string",
        "bigint": "double",
        "date": "string",
    }

    def __init__(self, column_name, converted_agate_type, specified_type):
        self.column_name = column_name
        self.converted_agate_type = converted_agate_type
        self.specified_type = specified_type

    def as_schema_value(self):
        return ColumnCsvMappingStrategy._schema_mappings.get(self.converted_agate_type, self.converted_agate_type)

    def as_cast_value(self):
        return self.specified_type if self.specified_type else self.converted_agate_type

    @classmethod
    def from_model(cls, model, agate_table):
        return [
            ColumnCsvMappingStrategy(
                column.name,
                GlueAdapter.convert_agate_type(agate_table, i),
                model.get("config", {}).get("column_types", {}).get(column.name),
            )
            for i, column in enumerate(agate_table.columns)
        ]
    
class GlueAdapter(SQLAdapter):
    ConnectionManager = GlueConnectionManager
    Relation = SparkRelation
    Column = GlueColumn

    relation_type_map = {'EXTERNAL_TABLE': 'table',
                         'MANAGED_TABLE': 'table',
                         'VIRTUAL_VIEW': 'view',
                         'table': 'table',
                         'view': 'view',
                         'cte': 'cte',
                         'materializedview': 'materializedview'}

    HUDI_METADATA_COLUMNS = [
        '_hoodie_commit_time',
        '_hoodie_commit_seqno',
        '_hoodie_record_key',
        '_hoodie_partition_path',
        '_hoodie_file_name'
    ]

    @classmethod
    def date_function(cls) -> str:
        return 'current_timestamp()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    def use_arrow(self):
        connection: GlueConnectionManager = self.connections.get_thread_connection()
        glueSession: GlueConnection = connection.handle
        return glueSession.credentials.use_arrow

    def get_connection(self):
        connection: GlueConnectionManager = self.connections.get_thread_connection()
        glueSession: GlueConnection = connection.handle
        if glueSession.credentials.role_arn:
            if glueSession.credentials.use_interactive_session_role_for_api_calls:
                sts_client = boto3.client('sts')
                assumed_role_object = sts_client.assume_role(
                    RoleArn=glueSession.credentials.role_arn,
                    RoleSessionName="dbt"
                )
                credentials = assumed_role_object['Credentials']
                glue_client = boto3.client("glue", region_name=glueSession.credentials.region,
                                           aws_access_key_id=credentials['AccessKeyId'],
                                           aws_secret_access_key=credentials['SecretAccessKey'],
                                           aws_session_token=credentials['SessionToken'])
                return glueSession, glue_client

        glue_client = boto3.client("glue", region_name=glueSession.credentials.region)
        return glueSession, glue_client

    def list_schemas(self, database: str) -> List[str]:
        session, client = self.get_connection()
        paginator = client.get_paginator('get_databases')
        schemas = []
        for page in paginator.paginate():
            databaseList = page['DatabaseList']
            for databaseDict in databaseList:
                databaseName = databaseDict['Name']
                schemas.append(databaseName)
        return schemas

    def list_relations_without_caching(self, schema_relation: SparkRelation):
        session, client = self.get_connection()
        relations = []
        paginator = client.get_paginator('get_tables')
        try:
            for page in paginator.paginate(DatabaseName=schema_relation.schema):
                for table in page.get('TableList', []):
                    relations.append(self.Relation.create(
                        database=schema_relation.schema,
                        schema=schema_relation.schema,
                        identifier=table.get("Name"),
                        type=self.relation_type_map.get(table.get("TableType")),
                    ))
            return relations
        except client.exceptions.EntityNotFoundException as e:
            return []
        except Exception as e:
            logger.error(e)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        try:
            list = self.list_schemas(schema)
            if schema in list:
                return True
            else:
                return False
        except Exception as e:
            logger.error(e)

    def check_relation_exists(self, relation: BaseRelation) -> bool:
        try:
            relation = self.get_relation(
                database=relation.schema,
                schema=relation.schema,
                identifier=relation.identifier
            )
            if relation is None:
                return False
            else:
                return True
        except Exception as e:
            logger.error(e)

    @available
    def glue_rename_relation(self, from_relation, to_relation):
        logger.debug("rename " + from_relation.schema + " to " + to_relation.identifier)
        session, client = self.get_connection()
        code = f'''
        custom_glue_code_for_dbt_adapter
        df = spark.sql("""select * from {from_relation.schema}.{from_relation.name}""")
        df.registerTempTable("df")
        table_name = '{to_relation.schema}.{to_relation.name}'
        writer = (
                        df.write.mode("append")
                        .format("parquet")
                        .option("path", "{session.credentials.location}/{to_relation.schema}/{to_relation.name}/")
                    )
        writer.saveAsTable(table_name, mode="append")
        spark.sql("""drop table {from_relation.schema}.{from_relation.name}""")
        SqlWrapper2.execute("""select * from {to_relation.schema}.{to_relation.name} limit 1""")
        '''
        try:
            session.cursor().execute(code)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueRenameRelationFailed") from e
        except Exception as e:
            logger.error(e)
            logger.error("rename_relation exception")

    def get_relation(self, database, schema, identifier):
        session, client = self.get_connection()
        if not identifier:
            logger.debug(f"get_relation returns None for schema : {schema} as identifier is not set")
            return None
        try:
            schema = self._strip_catalog_from_schema(schema)

            logger.debug(f"get_relation schema: {schema}, identifier: {identifier}")
            response = client.get_table(
                DatabaseName=schema,
                Name=identifier
            )
            is_delta = response.get('Table').get("Parameters").get("spark.sql.sources.provider") == "delta"

            # Compute the new schema based on the iceberg requirements
            computed_schema = self.__compute_schema_based_on_type(schema=schema, identifier=identifier)
            relation = self.Relation.create(
                database=computed_schema,
                schema=computed_schema,
                identifier=identifier,
                type=self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table")),
                is_delta=is_delta
            )
            logger.debug(f"""schema : {schema}
                             identifier : {identifier}
                             type : {self.relation_type_map.get(response.get('Table', {}).get('TableType', 'Table'))}
                        """)
            return relation
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
        except Exception as e:
            logger.error(e)

    def __compute_schema_based_on_type(self, schema, identifier):
        iceberg_catalog = self.get_custom_iceberg_catalog_namespace()
        current_relation = self.Relation.create(database=schema, schema=schema, identifier=identifier)
        existing_relation_type = self.get_table_type(current_relation)
        already_exist_iceberg = (existing_relation_type == 'iceberg_table')
        non_null_catalog = (iceberg_catalog is not None)
        if non_null_catalog and already_exist_iceberg:
            # We add the iceberg catalog is the following cases
            return iceberg_catalog + '.' + schema
        else:
            # Otherwise we keep the relation as it is
            return schema

    def get_columns_in_relation(self, relation: BaseRelation):
        logger.debug("get_columns_in_relation called")
        session, client = self.get_connection()

        schema = self._strip_catalog_from_schema(relation.schema)

        computed_schema = self.__compute_schema_based_on_type(schema=schema, identifier=relation.identifier)

        records = []
        columns = []
        existing_columns = []

        code = f"""describe {computed_schema}.{relation.identifier}"""
        logger.debug(f"code: {code}")

        try:
            response = session.cursor().execute(code)
            records = self.fetch_all_response(response)
        except Exception as e:
            error_msg = str(e)
            logger.debug(f"First attempt failed with error: {error_msg}")

            # Only try without schema if the error indicates table not found
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg:
                try:
                    code = f"""describe {relation.identifier}"""
                    logger.debug(f"Attempting describe without schema: {code}")
                    response = session.cursor().execute(code)
                    records = self.fetch_all_response(response)
                except DbtDatabaseError as e:
                    raise DbtDatabaseError(msg="GlueGetColumnsInRelationFailed") from e
                except Exception as e:
                    logger.error(f"Second attempt failed with error: {str(e)}")
                    raise
            else:
                # If it's not a table not found error, raise the original exception
                raise

        for record in records:
            column_name: str = record[0].strip()
            column_type: str = record[1].strip()
            if (
                column_name.lower() not in ["", "not partitioned"]
                and not column_name.startswith("#")
                and column_name not in existing_columns
            ):
                column = self.Column(column=column_name, dtype=column_type)
                columns.append(column)
                existing_columns.append(column_name)

        logger.debug("columns before strip:")
        logger.debug(columns)
        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]

        # strip partition columns.
        columns = [x for x in columns if not re.match(r"^Part \d+$", x.name)]

        logger.debug("columns after strip:")
        logger.debug(columns)
        return columns

    def fetch_all_response(self, response):
        logger.debug("fetch_all_response called")
        records = []
        use_arrow = self.use_arrow()

        logger.debug(f"fetch_all_response use_arrow={use_arrow}")
        if use_arrow:
            result_bucket = response.get("result_bucket")
            result_key = response.get("result_key")
            pdf = get_pandas_dataframe_from_result_file(result_bucket, result_key)
            results = pdf.to_dict('records')[0]
            items = results.get("results", [])
            columns = get_columns_from_result(results)
        else:
            items = response.get("results", [])
            columns = [column.get("name") for column in response.get("description")]
        logger.debug(f"fetch_all_response results: {columns}")
        for item in items:
            record = []
            for column in columns:
                record.append(item.get("data", {}).get(column, None))
            records.append(record)
        return records

    def quote(self, identifier: str) -> str:  # type: ignore
        return "`{}`".format(identifier)

    def set_table_properties(self, table_properties):
        if table_properties == 'empty':
            return ""
        else:
            table_properties_formatted = []
            for key in table_properties:
                table_properties_formatted.append("'" + key + "'='" + table_properties[key] + "'")
            if len(table_properties_formatted) > 0:
                table_properties_csv = ','.join(table_properties_formatted)
                return "TBLPROPERTIES (" + table_properties_csv + ")"
            else:
                return ""


    @available
    def duplicate_view(self, from_relation: BaseRelation, to_relation: BaseRelation, ):
        session, client = self.get_connection()
        code = f'''SHOW CREATE TABLE {from_relation.schema}.{from_relation.identifier}'''
        try:
            response = session.cursor().execute(code)
            records = self.fetch_all_response(response)
            for record in records:
                create_view_statement = record[0]
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueDuplicateViewFailed") from e
        except Exception as e:
            logger.error(e)
        target_query = create_view_statement.replace(from_relation.schema, to_relation.schema)
        target_query = target_query.replace(from_relation.identifier, to_relation.identifier)
        return target_query

    @available
    def get_location(self, relation: BaseRelation):
        session, client = self.get_connection()
        location = session.credentials.location
        if location is not None:
            # Apply the check to remove any trailing slashes from location
            # The old get_iceberg_location don't work for Windows users since os.path.join uses '\' in windows
            location = location.rstrip("/")
        return f"LOCATION '{location}/{relation.schema}/{relation.name}'"

    def drop_schema(self, relation: BaseRelation) -> None:
        session, client = self.get_connection()
        if self.check_schema_exists(relation.database, relation.schema):
            try:
                client.delete_database(Name=relation.schema)
                logger.debug("Successfull deleted schema ", relation.schema)
                self.connections.cleanup_all()
                return True
            except Exception as e:
                self.connections.cleanup_all()
                logger.error(e)
        else:
            logger.debug("No schema to delete")

    def create_schema(self, relation: BaseRelation):
        session, client = self.get_connection()
        lf = boto3.client("lakeformation", region_name=session.credentials.region)
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        account = identity.get("Account")
        if self.check_schema_exists(relation.database, relation.schema):
            logger.debug(f"Schema {relation.database} exists - nothing to do")
        else:
            try:
                # create when database does not exist
                client.create_database(
                    DatabaseInput={
                        "Name": relation.schema,
                        'Description': 'test dbt database',
                        'LocationUri': f"{session.credentials.location}/{relation.schema}/",
                    }
                )
                Entries = []
                for i, role_arn in enumerate([session.credentials.role_arn]):
                    Entries.append(
                        {
                            "Id": str(uuid.uuid4()),
                            "Principal": {"DataLakePrincipalIdentifier": role_arn},
                            "Resource": {
                                "Database": {
                                    # 'CatalogId': AWS_ACCOUNT,
                                    "Name": relation.schema,
                                }
                            },
                            "Permissions": [
                                "Alter".upper(),
                                "Create_table".upper(),
                                "Drop".upper(),
                                "Describe".upper(),
                            ],
                            "PermissionsWithGrantOption": [
                                "Alter".upper(),
                                "Create_table".upper(),
                                "Drop".upper(),
                                "Describe".upper(),
                            ],
                        }
                    )
                    Entries.append(
                        {
                            "Id": str(uuid.uuid4()),
                            "Principal": {"DataLakePrincipalIdentifier": role_arn},
                            "Resource": {
                                "Table": {
                                    "DatabaseName": relation.schema,
                                    "TableWildcard": {},
                                    "CatalogId": account
                                }
                            },
                            "Permissions": [
                                "Select".upper(),
                                "Insert".upper(),
                                "Delete".upper(),
                                "Describe".upper(),
                                "Alter".upper(),
                                "Drop".upper(),
                            ],
                            "PermissionsWithGrantOption": [
                                "Select".upper(),
                                "Insert".upper(),
                                "Delete".upper(),
                                "Describe".upper(),
                                "Alter".upper(),
                                "Drop".upper(),
                            ],
                        }
                    )
                lf.batch_grant_permissions(CatalogId=account, Entries=Entries)
            except Exception as e:
                logger.error(e)
                logger.error("create_schema exception")

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(relation_configs)

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                if len(schemas) == 0:
                    continue
                name = list(schemas)[0]
                fut = tpe.submit_connected(
                    self, name, self._get_one_catalog, info, [name], relation_configs
                )
                futures.append(fut)

            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            raise CompilationError(
                f'Expected only one schema in glue _get_one_catalog, found '
                f'{schemas}'
            )

        schema_base_relation = BaseRelation.create(
            schema=list(schemas)[0]
        )

        results = self.list_relations_without_caching(schema_base_relation)
        rows = []

        for relation_row in results:
            name = relation_row.name
            relation_type = relation_row.type

            table_info = self.get_columns_in_relation(relation_row)

            for table_row in table_info:
                rows.append([
                    schema_base_relation.schema,
                    schema_base_relation.schema,
                    name,
                    relation_type,
                    '',
                    '',
                    table_row.column,
                    '0',
                    table_row.dtype,
                    ''
                ])

        column_names = [
            'table_database',
            'table_schema',
            'table_name',
            'table_type',
            'table_comment',
            'table_owner',
            'column_name',
            'column_index',
            'column_type',
            'column_comment'
        ]
        table = agate.Table(rows, column_names)

        return table

    @available
    def get_custom_iceberg_catalog_namespace(self):
        session, _ = self.get_connection()
        catalog_namespace = session.credentials.custom_iceberg_catalog_namespace
        if catalog_namespace is None or catalog_namespace == '':
            return None
        else:
            return catalog_namespace
    
    @available
    def create_csv_table(self, model, agate_table):
        session, client = self.get_connection()
        logger.debug(model)
        f = io.StringIO("")
        agate_table.to_json(f)
        if session.credentials.seed_mode == "overwrite":
            mode = "True"
        else:
            mode = "False"

        csv_chunks = self._split_csv_records_into_chunks(json.loads(f.getvalue()))
        statements = self._map_csv_chunks_to_code(csv_chunks, session, model, mode, ColumnCsvMappingStrategy.from_model(model, agate_table))
        try:
            cursor = session.cursor()
            for statement in statements:
                cursor.execute(statement)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueCreateCsvFailed") from e
        except Exception as e:
            logger.error(e)

    def _map_csv_chunks_to_code(
        self,
        csv_chunks: List[List[dict]],
        session: GlueConnection,
        model,
        mode,
        column_mappings: List[ColumnCsvMappingStrategy],
    ):
        statements = []
        for i, csv_chunk in enumerate(csv_chunks):
            is_first = i == 0
            is_last = i == len(csv_chunks) - 1
            code = "custom_glue_code_for_dbt_adapter\n"
            if is_first:
                code += f"""
csv = {csv_chunk}
"""
            else:
                code += f"""
csv.extend({csv_chunk})        
"""
            if not is_last:
                code += f'''
SqlWrapper2.execute("""select 1""")
'''
            else:
                if session.credentials.enable_spark_seed_casting:
                    csv_schema = ", ".join(
                        [f"{mapping.column_name}: {mapping.as_schema_value()}" for mapping in column_mappings]
                    )

                    cast_columns = ", ".join(
                        [
                            f'"cast({mapping.column_name} as {mapping.as_cast_value()}) as {mapping.column_name}"'
                            for mapping in column_mappings
                            if (cast_value := mapping.as_cast_value())
                        ],
                    )

                    code += f"""
df = spark.createDataFrame(csv, "{csv_schema}")
df = df.selectExpr({cast_columns})
"""
                else:
                    code += """
df = spark.createDataFrame(csv)
"""
                code += f'''
table_name = '{model["schema"]}.{model["name"]}'
if (spark.sql("show tables in {model["schema"]}").where("tableName == lower('{model["name"]}')").count() > 0):
    df.write\
        .mode("{session.credentials.seed_mode}")\
        .format("{session.credentials.seed_format}")\
        .insertInto(table_name, overwrite={mode})
else:
    df.write\
        .option("path", "{session.credentials.location}/{model["schema"]}/{model["name"]}")\
        .format("{session.credentials.seed_format}")\
        .saveAsTable(table_name)
SqlWrapper2.execute("""select * from {model["schema"]}.{model["name"]} limit 1""")
'''
            statements.append(code)
        return statements

    def _split_csv_records_into_chunks(self, records: List[dict], target_size=60000):
        chunks = [[]]
        for record in records:
            if len(str([*chunks[-1], record])) > target_size:
                chunks.append([record])
            else:
                chunks[-1].append(record)
        return chunks

    def _update_additional_location(self, target_relation, location):
        session, client = self.get_connection()
        table_input = {}
        try:
            logger.debug(f"_update_additional_location schema: {target_relation.schema}, name: {session.credentials.delta_athena_prefix}_{target_relation.name}")
            table_input = client.get_table(
                DatabaseName=f"{target_relation.schema}",
                Name=f"{session.credentials.delta_athena_prefix}_{target_relation.name}",
            ).get("Table", {})
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
            pass
        except Exception as e:
            logger.error(e)

        try:
            # removing redundant keys from table_input
            del table_input['DatabaseName'], \
                table_input['CreateTime'], \
                table_input['UpdateTime'], \
                table_input['CreatedBy'], \
                table_input['IsRegisteredWithLakeFormation'], \
                table_input['CatalogId'], \
                table_input['VersionId']

            if 'AdditionalLocations' not in table_input['StorageDescriptor']:
                table_input['StorageDescriptor']['AdditionalLocations'] = [location]
            elif location not in table_input['StorageDescriptor']['AdditionalLocations']:
                table_input['StorageDescriptor']['AdditionalLocations'].append(location)
        except KeyError as e:
            logger.debug(e)
            pass
        try:
            client.update_table(
                DatabaseName=f'{target_relation.schema}',
                TableInput=table_input,
                SkipArchive=True
            )
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
            pass
        except Exception as e:
            logger.error(e)

    @available
    def delta_update_manifest(self, target_relation, custom_location, partition_by):
        session, client = self.get_connection()
        if custom_location == "empty":
            location = f"{session.credentials.location}/{target_relation.schema}/{target_relation.name}"
        else:
            location = custom_location

        if {session.credentials.delta_athena_prefix} is not None:
            run_msck_repair = f'''
            spark.sql("MSCK REPAIR TABLE {target_relation.schema}.headertoberepalced_{target_relation.name}")
            SqlWrapper2.execute("""select 1""")
            '''
            generate_symlink = f'''
            custom_glue_code_for_dbt_adapter
            from delta.tables import DeltaTable
            deltaTable = DeltaTable.forPath(spark, "{location}")
            deltaTable.generate("symlink_format_manifest")

            '''
            if partition_by is not None:
                update_manifest_code = generate_symlink + run_msck_repair
            else:
                update_manifest_code = generate_symlink + f'''SqlWrapper2.execute("""select 1""")'''
            try:
                session.cursor().execute(
                    re.sub("headertoberepalced", session.credentials.delta_athena_prefix, update_manifest_code))
            except DbtDatabaseError as e:
                raise DbtDatabaseError(msg="GlueDeltaUpdateManifestFailed") from e
            except Exception as e:
                logger.error(e)
            self._update_additional_location(target_relation, location)

    @available
    def delta_create_table(self, target_relation, request, primary_key, partition_key, custom_location, delta_create_table_write_options=None):
        session, client = self.get_connection()
        logger.debug(request)

        table_name = f'{target_relation.schema}.{target_relation.name}'
        if custom_location == "empty":
            location = f"{session.credentials.location}/{target_relation.schema}/{target_relation.name}"
        else:
            location = custom_location
        
        options_string = ""
        if delta_create_table_write_options:
            for key, value in delta_create_table_write_options.items():
                options_string += f'.option("{key}", "{value}")'

        create_table_query = f"""
CREATE TABLE {table_name}
USING delta
LOCATION '{location}'
        """

        write_data_header = f'''
custom_glue_code_for_dbt_adapter
spark.sql("""
{request}
""").write.format("delta").mode("overwrite"){options_string}'''

        write_data_footer = f'''.save("{location}")
SqlWrapper2.execute("""select 1""")
'''

        create_athena_table_header = f'''
custom_glue_code_for_dbt_adapter
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "{location}")
deltaTable.generate("symlink_format_manifest")
schema = deltaTable.toDF().schema
columns = (','.join([field.simpleString() for field in schema])).replace(':', ' ')
ddl = """CREATE EXTERNAL TABLE {target_relation.schema}.headertoberepalced_{target_relation.name} (""" + columns + """)
'''

        create_athena_table_footer = f'''
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{location}/_symlink_format_manifest/'"""
spark.sql(ddl)
'''
        if partition_key is not None:
            part_list = (', '.join(['`"{}"`'.format(field) for field in partition_key])).replace('`', '')
            write_data_partition = f'''.partitionBy({part_list})'''
            create_athena_table_partition = f'''
PARTITIONED BY ({part_list})
            '''
            run_msck_repair = f'''
spark.sql("MSCK REPAIR TABLE {target_relation.schema}.headertoberepalced_{target_relation.name}")
SqlWrapper2.execute("""select 1""")
            '''
            write_data_code = write_data_header + write_data_partition + write_data_footer
            create_athena_table = create_athena_table_header + create_athena_table_partition + create_athena_table_footer + run_msck_repair
        else:
            write_data_code = write_data_header + write_data_footer
            create_athena_table = create_athena_table_header + create_athena_table_footer + f'''SqlWrapper2.execute("""select 1""")'''

        try:
            session.cursor().execute(write_data_code)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueDeltaWriteTableFailed") from e
        except Exception as e:
            logger.error(e)

        try:
            session.cursor().execute(create_table_query)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueDeltaWriteTableFailed") from e
        except Exception as e:
            logger.error(e)

        if {session.credentials.delta_athena_prefix} is not None:
            try:
                session.cursor().execute(
                    re.sub("headertoberepalced", session.credentials.delta_athena_prefix, create_athena_table))
            except DbtDatabaseError as e:
                raise DbtDatabaseError(msg="GlueDeltaCreateTableFailed") from e
            except Exception as e:
                logger.error(e)
            self._update_additional_location(target_relation, location)

    def _strip_catalog_from_schema(self, schema):
        """
        Utility function to remove the iceberg catalog name from schema if present.

        Args:
            schema (str): The schema name, possibly prefixed with catalog name

        Returns:
            str: The schema name without catalog prefix
        """
        iceberg_catalog = self.get_custom_iceberg_catalog_namespace()

        # If no custom catalog is configured, return schema as is
        if not iceberg_catalog:
            return schema

        # If schema starts with catalog name followed by a dot, remove it
        if schema.startswith(f"{iceberg_catalog}."):
            return schema[len(iceberg_catalog) + 1:]

        return schema

    @available
    def get_table_type(self, relation):
        session, client = self.get_connection()

        schema = self._strip_catalog_from_schema(relation.schema)

        logger.debug(f"get_table_type schema: {schema}, name: {relation.name}")
        try:
            response = client.get_table(
                DatabaseName=schema,
                Name=relation.name
            )
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
            pass
        try:
            _type = self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            _specific_type = response.get("Table", {}).get('Parameters', {}).get('table_type', '')

            if _specific_type.lower() == 'iceberg':
                _type = 'iceberg_table'

            logger.debug("table_name : " + relation.name)
            logger.debug("table type : " + _type)
            return _type
        except Exception as e:
            return None

    def hudi_write(self, write_mode, session, target_relation, custom_location):
        if custom_location == "empty":
            return f'''outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('{write_mode}').save("{session.credentials.location}/{target_relation.schema}/{target_relation.name}/")'''
        else:
            return f'''outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('{write_mode}').save("{custom_location}/")'''

    @available
    def hudi_merge_table(self, target_relation, request, primary_key, partition_key, custom_location, hudi_config,
                         substitute_variables):
        session, client = self.get_connection()
        isTableExists = False
        if self.check_relation_exists(target_relation):
            isTableExists = True
        else:
            isTableExists = False

        # Test if variable hudi_config is NoneType
        if hudi_config is None:
            hudi_config = {}

        base_config = {
            'className': 'org.apache.hudi',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.write.precombine.field': 'update_hudi_ts',
            'hoodie.datasource.write.recordkey.field': primary_key,
            'hoodie.table.name': target_relation.name,
            'hoodie.datasource.hive_sync.database': target_relation.schema,
            'hoodie.datasource.hive_sync.table': target_relation.name,
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
        }

        if partition_key:
            partition_list = ','.join(partition_key)
            partition_config = {
                'hoodie.datasource.write.partitionpath.field': f'{partition_list}',
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                'hoodie.datasource.hive_sync.partition_fields': f'{partition_list}',
                'hoodie.index.type': 'GLOBAL_BLOOM',
                'hoodie.bloom.index.update.partition.path': 'true',
            }
        else:
            partition_config = {
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
                'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                'hoodie.index.type': 'GLOBAL_BLOOM',
                'hoodie.bloom.index.update.partition.path': 'true',
            }

        if isTableExists:
            write_mode = 'Append'
            write_operation_config = {
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                'hoodie.cleaner.commits.retained': 10,
            }
        else:
            write_mode = 'Overwrite'
            write_operation_config = {
                'hoodie.datasource.write.operation': 'bulk_insert',
            }

        combined_config = {**base_config, **partition_config, **write_operation_config, **hudi_config}

        code = f'''
custom_glue_code_for_dbt_adapter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.config("hoodie.metadata.enable", "true") \
.config("hoodie.enable.data.skipping", "true") \
.config("hoodie.metadata.index.column.stats.enable", "true") \
.config("hoodie.metadata.index.bloom.filter.enable", "true") \
.getOrCreate()
request = """{request}"""
substitute_variables = {str(substitute_variables)}
for index, value in enumerate(substitute_variables):
    request=eval(f"request.replace(f'<SUBSTITUTE_VARIABLE_{{index}}>',str(eval('{{value}}')))")
inputDf = spark.sql(request)
outputDf = inputDf.drop("dbt_unique_key").withColumn("update_hudi_ts",current_timestamp())
if outputDf.count() > 0:
        parallelism = spark.conf.get("spark.default.parallelism")
        print("spark.default.parallelism: %s", parallelism)
        hudi_parallelism_options = {{
            "hoodie.upsert.shuffle.parallelism": parallelism,
            "hoodie.bulkinsert.shuffle.parallelism": parallelism,
        }}
        combinedConf = {{**{str(combined_config)}, **hudi_parallelism_options, **{str(hudi_config)}}}
        {self.hudi_write(write_mode, session, target_relation, custom_location)}

spark.sql("""REFRESH TABLE {target_relation.schema}.{target_relation.name}""")
SqlWrapper2.execute("""SELECT * FROM {target_relation.schema}.{target_relation.name} LIMIT 1""")
        '''

        logger.debug(f"""hudi code :
        {code}
        """)

        try:
            session.cursor().execute(code)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueHudiMergeTableFailed") from e
        except Exception as e:
            logger.error(e)

    @available
    def iceberg_expire_snapshots(self, relation):
        """
        Helper function to call snapshot expiration.
        The function check for the latest snapshot and it expire all versions before it.
        If the table has only one snapshot it is retained.
        """
        session, client = self.get_connection()
        logger.debug(f'expiring snapshots for table {str(relation)}')
    
        catalog_extension = self.get_custom_iceberg_catalog_namespace()
        if catalog_extension is not None:
            expire_sql = f"CALL {catalog_extension}.system.expire_snapshots('{str(relation)}', timestamp 'to_replace')"
        else :
            expire_sql = f"CALL system.expire_snapshots('{str(relation)}', timestamp 'to_replace')"

        code = f'''
        custom_glue_code_for_dbt_adapter
        history_df = spark.sql("select committed_at from {relation}.snapshots order by committed_at desc")
        last_commited_at = str(history_df.first().committed_at)
        expire_sql_procedure = f"{expire_sql}".replace("to_replace", last_commited_at)
        result_df = spark.sql(expire_sql_procedure)
        SqlWrapper2.execute("""SELECT 1""")
        '''
        logger.debug(f"""expire procedure code:
            {code}
            """)
        try:
            session.cursor().execute(code)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueIcebergExpireSnapshotsFailed") from e
        except Exception as e:
            logger.error(e)

    @available
    def add_lf_tags(self, relation: SparkRelation, lf_tags_config: Dict[str, Any]) -> None:
        config = LfTagsConfig(**lf_tags_config)
        if config.enabled:
            conn = self.connections.get_thread_connection()
            client = conn.handle
            lf = boto3.client("lakeformation", region_name=client.credentials.region)
            sts = boto3.client("sts")
            identity = sts.get_caller_identity()
            account = identity.get("Account")
            manager = LfTagsManager(lf, account, relation, config)
            manager.process_lf_tags()
            return
        logger.debug(f"Lakeformation is disabled for {relation}")

    @available
    def apply_lf_grants(self, relation: SparkRelation, lf_grants_config: Dict[str, Any]) -> None:
        lf_config = LfGrantsConfig(**lf_grants_config)
        if lf_config.data_cell_filters.enabled:
            conn = self.connections.get_thread_connection()
            client = conn.handle
            lf = boto3.client("lakeformation", region_name=client.credentials.region)
            sts = boto3.client("sts")
            identity = sts.get_caller_identity()
            account = identity.get("Account")
            lf_permissions = LfPermissions(account, relation, lf)  # type: ignore
            lf_permissions.process_filters(lf_config)
            lf_permissions.process_permissions(lf_config)

    @available
    def execute_pyspark(self, codeblock):
        session, client = self.get_connection()

        code = f"""
custom_glue_code_for_dbt_adapter
{codeblock}
        """

        logger.debug(f"""pyspark code :
        {code}
        """)

        try:
            session.cursor().execute(code)
        except DbtDatabaseError as e:
            raise DbtDatabaseError(msg="GlueExecutePySparkFailed") from e
        except Exception as e:
            logger.error(e)