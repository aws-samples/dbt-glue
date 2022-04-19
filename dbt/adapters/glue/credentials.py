from dataclasses import dataclass
from typing import Optional
from dbt.adapters.base import Credentials

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
    glue_version: Optional[str] = "2.0"
    security_configuration: Optional[str] = None
    connections: Optional[str] = None
    
    @property
    def type(self):
        return "glue"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        """ Keys to show when debugging """
        return [
            'role_arn',
            'region',
            'session_id',
            'workers',
            'worker_type',
            'session_provisioning_timeout_in_seconds',
            'database',
            'schema',
            'location',
            'extra_jars',
            'idle_timeout',
            'glue_version',
            'security_configuration',
            'connections'
        ]
