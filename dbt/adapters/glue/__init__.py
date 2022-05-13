from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.connections import GlueConnectionManager
from dbt.adapters.glue.impl import GlueAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import glue

Plugin = AdapterPlugin(
    adapter=GlueAdapter,
    credentials=GlueCredentials,
    include_path=glue.PACKAGE_PATH,
    dependencies = ["spark"],
)
