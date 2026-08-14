from typing import Optional, Type, Any
from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import deep_merge


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = '`'
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    information: str = None

    @classmethod
    def create_from(
        cls: Type['SparkRelation'],
        quoting,
        relation_config,
        **kwargs: Any,
    ) -> 'SparkRelation':
        # If unset (None), it defaults to False. If explicitly set, that value is inherited.
        def _drop_none(policy_dict):
            return {k: v for k, v in (policy_dict or {}).items() if v is not None}

        quote_policy = _drop_none(kwargs.pop('quote_policy', {}))

        config_quoting = dict(relation_config.quoting_dict)
        config_quoting.pop('column', None)

        catalog_name = (
            relation_config.catalog_name
            if hasattr(relation_config, 'catalog_name')
            else relation_config.config.get('catalog', None)
        )

        merged_quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            _drop_none(quoting.quoting),
            _drop_none(config_quoting),
            quote_policy,
        )

        return cls.create(
            database=relation_config.database,
            schema=relation_config.schema,
            identifier=relation_config.identifier,
            quote_policy=merged_quote_policy,
            catalog_name=catalog_name,
            **kwargs,
        )

    def __post_init__(self):
        return
        if self.database != self.schema and self.database:
            raise DbtRuntimeError('Cannot set database in spark!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                'Got a spark relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()