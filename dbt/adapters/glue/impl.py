import io
import uuid
import boto3
from typing import List, Optional

import dbt
import agate
from concurrent.futures import Future

from dbt.adapters.base import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.column import Column
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.glue import GlueConnectionManager
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.relation import SparkRelation
from dbt.exceptions import NotImplementedException
from dbt.adapters.base.impl import catch_as_completed
from botocore.exceptions import ClientError
from dbt.utils import executor
from dbt.logger import GLOBAL_LOGGER as logger


class GlueAdapter(SQLAdapter):
    ConnectionManager = GlueConnectionManager
    Relation = SparkRelation

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

    def get_connection(self):
        connection: GlueConnectionManager = self.connections.get_thread_connection()
        session: GlueConnection = connection.handle
        client = boto3.client("glue", region_name=session.credentials.region)
        cursor = session.cursor()

        return session, client, cursor

    def list_schemas(self, database: str) -> List[str]:
        # results[1] contains the result of the query
        # row[0] get the first column of the result
        # here schemas contains all the databases in glue
        results = self.connections.execute("show databases", fetch=True)
        schemas = [row[0] for row in results[1]]
        return schemas

    def list_relations_without_caching(self, schema_relation: BaseRelation):
        session, client, cursor = self.get_connection()
        relations = []
        try:
            response = client.get_tables(
                DatabaseName=schema_relation.schema,
            )
            for table in response.get("TableList", []):
                relations.append(self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=table.get("Name"),
                    type=self.relation_type_map.get(table.get("TableType")),
                ))
        except Exception as e:
            logger.error(e)
            logger.error("list_relations_without_caching exception")

        return relations

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

    def check_schema_exists(self, database: str, schema: str) -> bool:

        try:
            list = list_schemas(schema)
            if 'schema' in list:
                return True
            else:
                return False
        except Exception as e:
            logger.error(e)
            logger.error("check_schema_exists exception")

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
            logger.error("check_relation_exists exception")


    def get_relation(self, database, schema, identifier):
        relations = []
        session, client, cursor = self.get_connection()
        try:
            response = client.get_table(
                DatabaseName=schema,
                Name=identifier
            )
            logger.debug(f"""debug type
                             {response.get('Table', {}).get('TableType', 'Table')}
            """)
            relations.append(self.Relation.create(
                schema=schema,
                identifier=identifier,
                type=self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            ))
            logger.debug(f"""schema : {schema}
                             identifier : {identifier}
                             type : {self.relation_type_map.get(response.get('Table', {}).get('TableType', 'Table'))}
            """)
            return relations
        except Exception as e:
            logger.error(e)
            logger.error(f"relation {schema}.{identifier} not found")

    @available
    def drop_view(self, relation: BaseRelation):
        session, client, cursor = self.get_connection()
        code = f'''DROP VIEW IF EXISTS {relation.schema}.{relation.name}'''
        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error("drop_view exception")
            logger.error(f"""drop_view exception
                             relation schema : {relation.schema}
                             relation identfier : {relation.name}
            """)

    @available
    def drop_relation(self, relations):
        session, client, cursor = self.get_connection()
        for relation in relations:
            code = f'''DROP TABLE IF EXISTS {relation}'''
            try:
                cursor.execute(code)
            except Exception as e:
                logger.error(e)
                logger.error(f"""drop_relation exception
                                 relation schema : {relation.schema}
                                 relation identfier : {relation.name}
                """)

    @available
    def create_view_as(self, relation: BaseRelation, sql: str):
        session, client, cursor = self.get_connection()
        if self.check_relation_exists(relation):
            code = f'''DROP TABLE IF EXISTS {relation.schema}.{relation.name}'''
            try:
                cursor.execute(code)
            except Exception as e:
                logger.error(e)
                logger.error(f"""drop table exception
                                 relation schema : {relation.schema}
                                 relation identfier : {relation.name}
                """)

        code = f'''
        create or replace view {relation.schema}.{relation.name}
        as
        {sql}
        '''

        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error(f"""create_view exception
                             relation schema : {relation.schema}
                             relation identfier : {relation.name}
            """)

        check_code = f'''select * from {relation.schema}.{relation.name} limit 1'''

        return check_code

    def rename_relation(self, from_relation, to_relation):
        logger.debug("rename " + from_relation.schema + " to " + to_relation.identifier)
        session, client, cursor = self.get_connection()
        code = f'''
        custom_glue_code_for_dbt_adapter
        df = spark.sql("""select * from {from_relation.schema}.{from_relation.name}""")
        df.registerTempTable("df")
        df = df.coalesce(1)
        table_name = '{to_relation.schema}.{to_relation.name}'
        writer = (
                        df.write.mode("append")
                        .format("parquet")
                        .option("path", "{session.credentials.location}/{to_relation.schema}/{to_relation.name}/")
                    )
        writer.saveAsTable(table_name, mode="append")
        '''
        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error("rename_relation exception")

    def get_columns_in_relation(self, relation: BaseRelation):
        logger.debug(f"Command launched: describe {relation.schema}.{relation.identifier}")
        session, client, cursor = self.get_connection()
        code = f'''describe {relation.schema}.{relation.identifier}'''
        columns = []
        try:
            cursor.execute(code)
            for record in cursor.fetchall():
                column = Column(column=record[0], dtype=record[1])
                if record[0][:1] != "#":
                    if column not in columns:
                        columns.append(column)
        except Exception as e:
            logger.error(e)
            logger.error("get_columns_in_relation exception")

        # strip hudi metadata columns.
        columns = [x for x in columns
                   if x.name not in self.HUDI_METADATA_COLUMNS]

        return columns

    @available
    def get_location(self, relation: BaseRelation):
        session, client, cursor = self.get_connection()
        return f"LOCATION '{session.credentials.location}/{relation.schema}/{relation.name}/'"

    def drop_schema(self, relation: BaseRelation) -> None:
        session, client, cursor = self.get_connection()
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
        session, client, cursor = self.get_connection()
        lf = boto3.client("lakeformation", region_name=session.credentials.region)
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        account = identity.get("Account")
        if self.check_schema_exists(relation.database, relation.schema):
            logger.debug(f"Schema {relation.database} exists - nothing to do")
        else:
            try:
                # create when database does not exist
                logger.debug("location = ", session.credentials.location)
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

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], manifest
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
            self, information_schema, schemas, manifest,
    ) -> agate.Table:
        logger.debug("_get_one_catalog called with args")
        logger.debug(schemas)
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in glue _get_one_catalog, found '
                f'{schemas}'
            )

        schema_base_relation = BaseRelation.create(
            schema=list(schemas)[0]
        )

        logger.debug("Schemas : " + schema_base_relation.schema)
        results = self.list_relations_without_caching(schema_base_relation)
        rows = []

        for relation_row in results:
            name = relation_row.name
            relation_type = relation_row.type

            table_info = self.get_columns_in_relation(relation_row)

            for table_row in table_info:
                rows.append([
                    information_schema.database,
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

        return self._catalog_filter_table(table, manifest)

    @available
    def create_csv_table(self, model, agate_table):
        session, client, cursor = self.get_connection()
        logger.debug(model)
        f = io.StringIO("")
        agate_table.to_json(f)
        code = f'''
custom_glue_code_for_dbt_adapter
csv = {f.getvalue()}
df = spark.createDataFrame(csv)
table_name = '{model["schema"]}.{model["name"]}'
writer = (
    df.write.mode("append")
    .format("parquet")
    .option("path", "{session.credentials.location}/{model["schema"]}/{model["name"]}")
)
writer.saveAsTable(table_name, mode="append")
SqlWrapper2.execute("""select * from {model["schema"]}.{model["name"]}""")
'''
        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error("create_csv_table exception")


    def add_schema_to_cache(self, schema) -> str:
        if schema is None:
            name = self.nice_connection_name()
            dbt.exceptions.raise_compiler_error(
                'Attempted to cache a null schema for {}'.format(name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ''

    @available
    def describe_table(self, relation):
        session, client, cursor = self.get_connection()
        relations = []
        try:
            response = client.get_table(
                DatabaseName=relation.schema,
                Name=relation.name
            )
            relations.append(self.Relation.create(
                schema=relation.schema,
                identifier=relation.name,
                type=self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            ))
            logger.debug(f"""table_name : {relation.name}
                             table type {self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))}
            """)
            return relations
        except Exception:
            return None

    @available
    def get_table_type(self, relation):
        session, client, cursor = self.get_connection()
        try:
            response = client.get_table(
                DatabaseName=relation.schema,
                Name=relation.name
            )
            type = self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            logger.debug("table_name : " + relation.name)
            logger.debug("table type : " + type)
            return type
        except Exception as e:
            return None

    def get_rows_different_sql(
            self,
            relation_a: BaseRelation,
            relation_b: BaseRelation,
            column_names: Optional[List[str]] = None,
            except_operator: str = 'EXCEPT',
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def hudi_write(self, write_mode, session, target_relation):
        return f'''outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('{write_mode}').save("{session.credentials.location}/{target_relation.schema}/{target_relation.name}/")'''

    @available
    def hudi_merge_table(self, target_relation, request, primary_key, partition_key):
        session, client, cursor = self.get_connection()
        isTableExists = False
        if self.check_relation_exists(target_relation):
            isTableExists = True
            logger.debug(target_relation.schema + '.' + target_relation.name + ' exists.')
        else:
            isTableExists = False
            logger.debug(
                 target_relation.schema + '.' + target_relation.name + ' does not exist. Table will be created.')

        head_code = f'''
custom_glue_code_for_dbt_adapter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.getOrCreate()
inputDf = spark.sql("""{request}""")
outputDf = inputDf.withColumn("update_hudi_ts",current_timestamp())
if outputDf.count() > 0:
    if {partition_key} is not None:
        outputDf = outputDf.withColumn(partitionKey, concat(lit(partitionKey + '='), col(partitionKey)))
        '''

        begin_of_hudi_setup = f'''combinedConf = {{'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'update_hudi_ts', 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.write.recordkey.field': '{primary_key}', 'hoodie.table.name': '{target_relation.name}', 'hoodie.datasource.hive_sync.database': '{target_relation.schema}', 'hoodie.datasource.hive_sync.table': '{target_relation.name}', 'hoodie.datasource.hive_sync.enable': 'true','''

        hudi_partitionning = f''' 'hoodie.datasource.write.partitionpath.field': '{partition_key}', 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor', 'hoodie.datasource.hive_sync.partition_fields': '{partition_key}','''

        hudi_no_partition = f''' 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator','''

        hudi_upsert = f''' 'hoodie.upsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}}'''

        hudi_insert = f''' 'hoodie.bulkinsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'bulk_insert'}}'''

        if isTableExists:
            write_mode = "Append"
            core_code = f'''
        {begin_of_hudi_setup} {hudi_partitionning} {hudi_upsert}
        {self.hudi_write(write_mode, session, target_relation)}
    else:
        {begin_of_hudi_setup} {hudi_no_partition} {hudi_upsert}
        {self.hudi_write(write_mode, session, target_relation)}
        '''
        else:
            write_mode = "Overwrite"
            core_code = f'''
        {begin_of_hudi_setup} {hudi_partitionning} {hudi_insert}
        {self.hudi_write(write_mode, session, target_relation)}
    else:
        {begin_of_hudi_setup} {hudi_no_partition} {hudi_insert}
        {self.hudi_write(write_mode, session, target_relation)}
        '''

        footer_code = f'''
spark.sql("""REFRESH TABLE {target_relation.schema}.{target_relation.name}""")
SqlWrapper2.execute("""SELECT * FROM {target_relation.schema}.{target_relation.name} LIMIT 1""")
        '''

        code = head_code + core_code + footer_code
        logger.debug(f"""hudi code :
        {code}
        """)

        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error("hudi_merge_table exception")


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = '''
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
cross join diff_count
'''.strip()
