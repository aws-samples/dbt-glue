from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.base.column import Column


@dataclass
class GlueColumn(Column):
    # Overwriting dafult string types to support glue
    # TODO: Convert to supported glue types as needed
    # Please ref: https://github.com/dbt-athena/dbt-athena/blob/main/dbt/adapters/athena/column.py
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "STRING",
        "TEXT": "STRING",
        "VARCHAR": "STRING"
    }
