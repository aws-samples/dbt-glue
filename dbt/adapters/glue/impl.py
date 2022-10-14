import io
import re
import uuid
import boto3
from typing import List

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
from dbt.utils import executor
from dbt.events import AdapterLogger

logger = AdapterLogger("Glue")


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

    def get_connection(self):
        connection: GlueConnectionManager = self.connections.get_thread_connection()
        session: GlueConnection = connection.handle
        client = boto3.client("glue", region_name=session.credentials.region)
        cursor = session.cursor()

        return session, client, cursor

    def list_schemas(self, database: str) -> List[str]:
        session, client, cursor = self.get_connection()
        responseGetDatabases = client.get_databases()
        databaseList = responseGetDatabases['DatabaseList']
        schemas = []
        for databaseDict in databaseList:
            databaseName = databaseDict['Name']
            schemas.append(databaseName)
        return schemas

    def list_relations_without_caching(self, schema_relation: SparkRelation):
        session, client, cursor = self.get_connection()
        relations = []
        try:
            response = client.get_tables(
                DatabaseName=schema_relation.schema,
            )
            for table in response.get("TableList", []):
                relations.append(self.Relation.create(
                    database=schema_relation.schema,
                    schema=schema_relation.schema,
                    identifier=table.get("Name"),
                    type=self.relation_type_map.get(table.get("TableType")),
                ))
            return relations
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
        session, client, cursor = self.get_connection()
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
            cursor.execute(code)
        except Exception as e:
            logger.error(e)
            logger.error("rename_relation exception")

    def get_relation(self, database, schema, identifier):
        session, client, cursor = self.get_connection()
        try:
            response = client.get_table(
                DatabaseName=schema,
                Name=identifier
            )
            relations = self.Relation.create(
                database=schema,
                schema=schema,
                identifier=identifier,
                type=self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            )
            logger.debug(f"""schema : {schema}
                             identifier : {identifier}
                             type : {self.relation_type_map.get(response.get('Table', {}).get('TableType', 'Table'))}
                        """)
            return relations
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
        except Exception as e:
            logger.error(e)

    def get_columns_in_relation(self, relation: BaseRelation) -> [Column]:
        session, client, cursor = self.get_connection()
        # https://spark.apache.org/docs/3.0.0/sql-ref-syntax-aux-describe-table.html
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

        # strip hudi metadata columns.
        columns = [x for x in columns
                   if x.name not in self.HUDI_METADATA_COLUMNS]

        return columns

    @available
    def duplicate_view(self, from_relation: BaseRelation, to_relation: BaseRelation, ):
        session, client, cursor = self.get_connection()
        code = f'''SHOW CREATE TABLE {from_relation.schema}.{from_relation.identifier}'''
        try:
            cursor.execute(code)
            for record in cursor.fetchall():
                create_view_statement = record[0]
        except Exception as e:
            logger.error(e)
        target_query = create_view_statement.replace(from_relation.schema, to_relation.schema)
        target_query = target_query.replace(from_relation.identifier, to_relation.identifier)
        return target_query

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

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                if len(schemas) == 0:
                    continue
                name = list(schemas)[0]
                fut = tpe.submit_connected(
                    self, name, self._get_one_catalog, info, [name], manifest
                )
                futures.append(fut)

            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
            self, information_schema, schemas, manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
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
SqlWrapper2.execute("""select * from {model["schema"]}.{model["name"]} limit 1""")
'''
        try:
            cursor.execute(code)
        except Exception as e:
            logger.error(e)

    @available
    def delta_update_manifest(self, target_relation, custom_location):
        session, client, cursor = self.get_connection()
        if custom_location == "empty":
            location = f"{session.credentials.location}/{target_relation.schema}/{target_relation.name}"
        else:
            location = custom_location

        if {session.credentials.delta_athena_prefix} is not None:
            update_manifest_code = f'''
            custom_glue_code_for_dbt_adapter
            from delta.tables import DeltaTable
            deltaTable = DeltaTable.forPath(spark, "{location}")
            deltaTable.generate("symlink_format_manifest")
            spark.sql("MSCK REPAIR TABLE {target_relation.schema}.headertoberepalced_{target_relation.name}") 
            SqlWrapper2.execute("""select 1""")
            '''

            try:
                cursor.execute(re.sub("headertoberepalced", session.credentials.delta_athena_prefix, update_manifest_code))
            except Exception as e:
                logger.error(e)



    @available
    def delta_create_table(self, target_relation, request, primary_key, partition_key, custom_location):
        session, client, cursor = self.get_connection()
        logger.debug(request)

        table_name = f'{target_relation.schema}.{target_relation.name}'
        if custom_location == "empty":
            location = f"{session.credentials.location}/{target_relation.schema}/{target_relation.name}"
        else:
            location = custom_location

        create_table_query = f"""
CREATE TABLE {table_name}
USING delta
LOCATION '{location}'
        """

        write_data_header = f'''
custom_glue_code_for_dbt_adapter
spark.sql("""
{request}
""").write.format("delta").mode("overwrite")'''

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
spark.sql("MSCK REPAIR TABLE {target_relation.schema}.headertoberepalced_{target_relation.name}") 
SqlWrapper2.execute("""select 1""")
                        '''
        if partition_key is not None:
            part_list = (', '.join([field for field in partition_key]))
            write_data_partition = f'''.partitionBy("{part_list}")'''
            create_athena_table_partition = f'''
PARTITIONED BY ({part_list})
            '''
            write_data_code = write_data_header + write_data_partition + write_data_footer
            create_athena_table = create_athena_table_header + create_athena_table_partition + create_athena_table_footer
        else:
            write_data_code = write_data_header + write_data_footer
            create_athena_table = create_athena_table_header + create_athena_table_footer



        try:
            cursor.execute(write_data_code)
        except Exception as e:
            logger.error(e)

        try:
            cursor.execute(create_table_query)
        except Exception as e:
            logger.error(e)

        if {session.credentials.delta_athena_prefix} is not None:
            try:
                cursor.execute(re.sub("headertoberepalced", session.credentials.delta_athena_prefix, create_athena_table))
            except Exception as e:
                logger.error(e)

    @available
    def get_table_type(self, relation):
        session, client, cursor = self.get_connection()
        try:
            response = client.get_table(
                DatabaseName=relation.schema,
                Name=relation.name
            )
        except client.exceptions.EntityNotFoundException as e:
            logger.debug(e)
            pass
        try:
            type = self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            logger.debug("table_name : " + relation.name)
            logger.debug("table type : " + type)
            return type
        except Exception as e:
            return None

    def hudi_write(self, write_mode, session, target_relation, custom_location):
        if custom_location == "empty":
            return f'''outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('{write_mode}').save("{session.credentials.location}/{target_relation.schema}/{target_relation.name}/")'''
        else:
            return f'''outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('{write_mode}').save("{custom_location}/")'''

    @available
    def hudi_merge_table(self, target_relation, request, primary_key, partition_key, custom_location):
        session, client, cursor = self.get_connection()
        isTableExists = False
        if self.check_relation_exists(target_relation):
            isTableExists = True
        else:
            isTableExists = False

        if partition_key:
            partition_list = ','.join(partition_key)
            hudi_partitionning = f''' 'hoodie.datasource.write.partitionpath.field': '{partition_list}', 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor', 'hoodie.datasource.hive_sync.partition_fields': '{partition_list}','''
        else:
            hudi_partitionning = ''


        begin_of_hudi_setup = f'''combinedConf = {{'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'update_hudi_ts', 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.write.recordkey.field': '{primary_key}', 'hoodie.table.name': '{target_relation.name}', 'hoodie.datasource.hive_sync.database': '{target_relation.schema}', 'hoodie.datasource.hive_sync.table': '{target_relation.name}', 'hoodie.datasource.hive_sync.enable': 'true','''

        hudi_no_partition = f''' 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator','''

        hudi_upsert = f''' 'hoodie.upsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}}'''

        hudi_insert = f''' 'hoodie.bulkinsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'bulk_insert'}}'''

        head_code = f'''
custom_glue_code_for_dbt_adapter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.getOrCreate()
inputDf = spark.sql("""{request}""")
outputDf = inputDf.drop("dbt_unique_key").withColumn("update_hudi_ts",current_timestamp())
if outputDf.count() > 0:
    if {partition_key} is not None:
        '''
        if isTableExists:
            write_mode = "Append"
            core_code = f'''
        {begin_of_hudi_setup} {hudi_partitionning} {hudi_upsert}
        {self.hudi_write(write_mode, session, target_relation, custom_location)}
    else:
        {begin_of_hudi_setup} {hudi_no_partition} {hudi_upsert}
        {self.hudi_write(write_mode, session, target_relation, custom_location)}
        '''
        else:
            write_mode = "Overwrite"
            core_code = f'''
        {begin_of_hudi_setup} {hudi_partitionning} {hudi_insert}
        {self.hudi_write(write_mode, session, target_relation, custom_location)}
    else:
        {begin_of_hudi_setup} {hudi_no_partition} {hudi_insert}
        {self.hudi_write(write_mode, session, target_relation, custom_location)}
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
