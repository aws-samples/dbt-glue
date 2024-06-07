from dataclasses import dataclass
from typing import Optional
from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtRuntimeError

@dataclass
class GlueCredentials(Credentials):
    """ Required connections for a Glue connection"""
    role_arn: Optional[str] = None  # type: ignore
    region: Optional[str] = None  # type: ignore
    workers: Optional[int] = None  # type: ignore
    worker_type: Optional[str] = None  # type: ignore
    session_provisioning_timeout_in_seconds: int = 120
    location: Optional[str] = None
    extra_jars: Optional[str] = None
    idle_timeout: int = 10
    query_timeout_in_minutes: int = 300
    glue_version: Optional[str] = "4.0"
    security_configuration: Optional[str] = None
    connections: Optional[str] = None
    conf: Optional[str] = None
    extra_py_files: Optional[str] = None
    delta_athena_prefix: Optional[str] = None
    tags: Optional[str] = None
    database: Optional[str] = None  # type: ignore
    schema: Optional[str] = None  # type: ignore
    seed_format: Optional[str] = "parquet"
    seed_mode: Optional[str] = "overwrite"
    default_arguments: Optional[str] = None
    iceberg_glue_commit_lock_table: Optional[str] = "myGlueLockTable"
    use_interactive_session_role_for_api_calls: Optional[bool] = False
    lf_tags: Optional[str] = None
    glue_session_id: Optional[str] = None
    glue_session_reuse: Optional[bool] = False
    datalake_formats: Optional[str] = None
    enable_session_per_model: Optional[bool] = False
    use_arrow: Optional[bool] = False


    @property
    def type(self):
        return "glue"

    @property
    def unique_field(self):
        return "glue"

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

    def _connection_keys(self):
        """ Keys to show when debugging """
        return [
            'role_arn',
            'region',
            'workers',
            'worker_type',
            'session_provisioning_timeout_in_seconds',
            'schema',
            'location',
            'extra_jars',
            'idle_timeout',
            'query_timeout_in_minutes',
            'glue_version',
            'security_configuration',
            'connections',
            'conf',
            'extra_py_files',
            'delta_athena_prefix',
            'tags',
            'seed_format',
            'seed_mode',
            'default_arguments',
            'iceberg_glue_commit_lock_table',
            'use_interactive_session_role_for_api_calls',
            'lf_tags',
            'glue_session_id',
            'glue_session_reuse',
            'datalake_formats',
            'enable_session_per_model',
            'use_arrow'
        ]
