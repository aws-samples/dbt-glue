from typing import Optional, Type, Any
from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt_common.exceptions import DbtRuntimeError


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
        # Override quoting defaults because dbt core will deep_merge `None` values over True
        rel = super().create_from(quoting, relation_config, **kwargs)
        
        # Force default true for quote_policy if not explicitly disabled in project
        quote_policy = rel.quote_policy
        if getattr(quote_policy, 'database', None) is None:
            quote_policy.database = True
        if getattr(quote_policy, 'schema', None) is None:
            quote_policy.schema = True
        if getattr(quote_policy, 'identifier', None) is None:
            quote_policy.identifier = True
            
        return rel

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