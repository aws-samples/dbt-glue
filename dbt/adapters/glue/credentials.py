from dataclasses import dataclass
from typing import Optional
from dbt.adapters.base import Credentials
import dbt.exceptions

@dataclass
class GlueCredentials(Credentials):
    """ Required connections for a Glue connection"""
    role_arn: str
    region: str
    workers: int
    worker_type: str
    session_provisioning_timeout_in_seconds: int = 120
    session_id: Optional[str] = None
    location: Optional[str] = None
    extra_jars: Optional[str] = None
    idle_timeout: int = 10
    query_timeout_in_seconds: int = 300
    glue_version: Optional[str] = "3.0"
    security_configuration: Optional[str] = None
    connections: Optional[str] = None
    conf: Optional[str] = None
    extra_py_files: Optional[str] = None
    delta_athena_prefix: Optional[str] = None
    tags: Optional[str] = None
    database: Optional[str] # type: ignore

    @property
    def type(self):
        return "glue"

    @property
    def unique_field(self):
        return self.host

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
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
            'session_id',
            'workers',
            'worker_type',
            'session_provisioning_timeout_in_seconds',
            'schema',
            'location',
            'extra_jars',
            'idle_timeout',
            'query_timeout_in_seconds',
            'glue_version',
            'security_configuration',
            'connections',
            'conf',
            'extra_py_files',
            'delta_athena_prefix',
            'tags'
        ]
