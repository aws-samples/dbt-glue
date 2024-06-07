"""AWS Lakeformation permissions management helper utilities."""

from typing import Dict, List, Optional, Union

from dbt.adapters.glue.relation import SparkRelation
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Glue")


class LfTagsConfig:
    def __init__(self, enabled: bool = False, drop_existing: bool = False, tags_table: Optional[Dict[str, str]] = None, tags_database: Optional[Dict[str, str]] = None, tags_columns: Optional[Dict[str, Dict[str, List[str]]]] = None):
        self.enabled = enabled
        self.drop_existing = drop_existing
        self.tags_table = tags_table
        self.tags_columns = tags_columns
        self.tags_database = tags_database


class LfTagsManager:
    def __init__(self, lf_client, catalog_id, relation: SparkRelation, lf_tags_config: LfTagsConfig):
        self.lf_client = lf_client
        self.catalog_id = catalog_id
        self.database = relation.schema
        self.table = relation.identifier
        self.drop_existing = lf_tags_config.drop_existing
        self.lf_tags_table = lf_tags_config.tags_table
        self.lf_tags_database = lf_tags_config.tags_database
        self.lf_tags_columns = lf_tags_config.tags_columns

    def process_lf_tags(self) -> None:
        if self.lf_tags_database:
            db_resource = {
            "Database": {"CatalogId": self.catalog_id, "Name": self.database}}
            existing_lf_tags_database = self.lf_client.get_resource_lf_tags(
            Resource=db_resource)
            if self.drop_existing:
                self._remove_lf_tags_database(db_resource, existing_lf_tags_database)
            self._apply_lf_tags_database(db_resource)
        table_resource = {
            "Table": {"DatabaseName": self.database, "Name": self.table}}
        existing_lf_tags = self.lf_client.get_resource_lf_tags(
            Resource=table_resource)
        if self.drop_existing:
            self._remove_lf_tags_columns(existing_lf_tags)
        self._apply_lf_tags_table(table_resource, existing_lf_tags)
        self._apply_lf_tags_columns()

    def _remove_lf_tags_columns(self, existing_lf_tags) -> None:
        lf_tags_columns = existing_lf_tags.get("LFTagsOnColumns", [])
        logger.debug(f"COLUMNS: {lf_tags_columns}")
        if lf_tags_columns:
            to_remove = {}
            for column in lf_tags_columns:
                for tag in column["LFTags"]:
                    tag_key = tag["TagKey"]
                    tag_value = tag["TagValues"][0]
                    if tag_key not in to_remove:
                        to_remove[tag_key] = {tag_value: [column["Name"]]}
                    elif tag_value not in to_remove[tag_key]:
                        to_remove[tag_key][tag_value] = [column["Name"]]
                    else:
                        to_remove[tag_key][tag_value].append(column["Name"])
            logger.debug(f"TO REMOVE: {to_remove}")
            for tag_key, tag_config in to_remove.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {"DatabaseName": self.database, "Name": self.table, "ColumnNames": columns}
                    }
                    response = self.lf_client.remove_lf_tags_from_resource(
                        Resource=resource, LFTags=[
                            {"TagKey": tag_key, "TagValues": [tag_value]}]
                    )
                    logger.debug(self._parse_lf_response(
                        response, columns, {tag_key: tag_value}, "remove"))
    
    def _remove_lf_tags_database(self, db_resource, existing_lf_tags) -> None:
        lf_tags_database = existing_lf_tags.get("LFTagOnDatabase", [])
        logger.debug(f"DATABASE TAGS: {lf_tags_database}")
        to_remove = {
                tag["TagKey"]: tag["TagValues"]
                for tag in lf_tags_database
                if tag["TagKey"] not in self.lf_tags_database  # type: ignore
            }
        logger.debug(f"TAGS TO REMOVE: {to_remove}")
        if to_remove:
                response = self.lf_client.remove_lf_tags_from_resource(
                    Resource=db_resource, LFTags=[
                        {"TagKey": k, "TagValues": v} for k, v in to_remove.items()]
                )
                logger.debug(self._parse_lf_response(
                    response, None, to_remove, "remove"))
    
    def _apply_lf_tags_database(
            self, db_resource) -> None:
        if self.lf_tags_database:
            self.lf_client
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=db_resource, LFTags=[
                    {"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags_database.items()]
            )
            logger.debug(self._parse_lf_response(response, None, self.lf_tags_database))
            
    def _apply_lf_tags_table(
            self, table_resource, existing_lf_tags) -> None:
        existing_lf_tags_table = existing_lf_tags.get("LFTagsOnTable", [])
        logger.debug(f"EXISTING TABLE TAGS: {existing_lf_tags_table}")
        logger.debug(f"CONFIG TAGS: {self.lf_tags_table}")
        if self.drop_existing:
            to_remove = {
                tag["TagKey"]: tag["TagValues"]
                for tag in existing_lf_tags_table
                if tag["TagKey"] not in self.lf_tags_table  # type: ignore
            }
            logger.debug(f"TAGS TO REMOVE: {to_remove}")
            if to_remove:
                response = self.lf_client.remove_lf_tags_from_resource(
                    Resource=table_resource, LFTags=[
                        {"TagKey": k, "TagValues": v} for k, v in to_remove.items()]
                )
                logger.debug(self._parse_lf_response(
                    response, None, to_remove, "remove"))

        if self.lf_tags_table:
            self.lf_client
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=table_resource, LFTags=[
                    {"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags_table.items()]
            )
            logger.debug(self._parse_lf_response(response, None, self.lf_tags_table))

    def _apply_lf_tags_columns(self) -> None:
        if self.lf_tags_columns:
            for tag_key, tag_config in self.lf_tags_columns.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {"DatabaseName": self.database, "Name": self.table, "ColumnNames": columns}
                    }
                    response = self.lf_client.add_lf_tags_to_resource(
                        Resource=resource,
                        LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}],
                    )
                    logger.debug(self._parse_lf_response(
                        response, columns, {tag_key: tag_value}))

    def _parse_lf_response(
        self,
        response,
        columns: Optional[List[str]] = None,
        lf_tags: Optional[Dict[str, str]] = None,
        verb: str = "add",
    ) -> str:
        failures = response.get("Failures", [])
        columns_appendix = f" for columns {columns}" if columns else ""
        if failures:
            base_msg = f"Failed to {verb} LF tags: {lf_tags} to {self.database}.{self.table}" + columns_appendix
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                logger.error(
                    f"Failed to {verb} {tag} for {self.database}.{self.table}" + f" - {error}")
            raise DbtRuntimeError(base_msg)
        return f"Success: {verb} LF tags: {lf_tags} to {self.database}.{self.table}" + columns_appendix


class FilterConfig:
    def __init__(self, row_filter: str, column_names: List[str] = [], principals: List[str] = [], excluded_column_names: List[str] = []):
        self.row_filter = row_filter
        self.column_names = column_names
        self.principals = principals
        self.excluded_column_names = excluded_column_names

    def to_api_repr(self, catalog_id: str, database: str, table: str, name: str):
        if self.column_names != []:
            return {
                "TableCatalogId": catalog_id,
                "DatabaseName": database,
                "TableName": table,
                "Name": name,
                "RowFilter": {"FilterExpression": self.row_filter},
                "ColumnNames": self.column_names,
            }
        elif self.excluded_column_names != []:
            return {
                "TableCatalogId": catalog_id,
                "DatabaseName": database,
                "TableName": table,
                "Name": name,
                "RowFilter": {"FilterExpression": self.row_filter},
                "ColumnWildcard": {"ExcludedColumnNames": self.excluded_column_names}
            }
        else:
            return {
                "TableCatalogId": catalog_id,
                "DatabaseName": database,
                "TableName": table,
                "Name": name,
                "RowFilter": {"FilterExpression": self.row_filter},
                "ColumnWildcard": {"ExcludedColumnNames": []}
            }

    def to_update(self, existing) -> bool:
        return self.row_filter != existing["RowFilter"]["FilterExpression"] or set(self.column_names) != set(existing["ColumnNames"]) or set(self.excluded_column_names) != set(existing["ColumnWildcard"]['ExcludedColumnNames'])


class DataCellFiltersConfig:
    def __init__(self, enabled: bool = False, drop_existing: bool = False, filters: Dict[str, FilterConfig] = {}):
        self.enabled = enabled
        self.drop_existing = drop_existing
        self.filters = filters


class LfGrantsConfig:
    def __init__(self, data_cell_filters):
        self.data_cell_filters = DataCellFiltersConfig(data_cell_filters.get(
            'enabled', False), data_cell_filters.get('drop_existing', False), data_cell_filters.get('filters', {}))


class LfPermissions:
    def __init__(self, catalog_id: str, relation: SparkRelation, lf_client) -> None:
        self.catalog_id = catalog_id
        self.relation = relation
        self.database: str = relation.schema
        self.table: str = relation.identifier
        self.lf_client = lf_client

    def get_filters(self):
        table_resource = {"CatalogId": self.catalog_id,
                          "DatabaseName": self.database, "Name": self.table}
        return {f["Name"]: f for f in self.lf_client.list_data_cells_filter(Table=table_resource)["DataCellsFilters"]}

    def process_filters(self, config: LfGrantsConfig) -> None:
        current_filters = self.get_filters()
        logger.debug(f"CURRENT FILTERS: {current_filters}")
        if config.data_cell_filters.drop_existing:
            to_drop = [f for name, f in current_filters.items(
            ) if name not in config.data_cell_filters.filters]
            logger.debug(f"FILTERS TO DROP: {to_drop}")
            for f in to_drop:
                self.lf_client.delete_data_cells_filter(
                    TableCatalogId=f["TableCatalogId"],
                    DatabaseName=f["DatabaseName"],
                    TableName=f["TableName"],
                    Name=f["Name"],
                )

        to_add = [
            FilterConfig(row_filter=f.get('row_filter'), principals=f.get('principals'), column_names=f.get('column_names', [
            ]), excluded_column_names=f.get('excluded_column_names', [])).to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name not in current_filters
        ]
        logger.debug(f"FILTERS TO ADD: {to_add}")
        for f in to_add:
            self.lf_client.create_data_cells_filter(TableData=f)

        to_update = [
            FilterConfig(row_filter=f.get('row_filter'), principals=f.get('principals'), column_names=f.get('column_names', [
            ]), excluded_column_names=f.get('excluded_column_names', [])).to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name in current_filters and FilterConfig(row_filter=f.get('row_filter'), principals=f.get('principals'), column_names=f.get('column_names', []), excluded_column_names=f.get('excluded_column_names', [])).to_update(current_filters[name])
        ]
        logger.debug(f"FILTERS TO UPDATE: {to_update}")
        for f in to_update:
            self.lf_client.update_data_cells_filter(TableData=f)

    def process_permissions(self, config: LfGrantsConfig) -> None:
        for name, f in config.data_cell_filters.filters.items():
            filterConfig = FilterConfig(row_filter=f.get('row_filter'), principals=f.get('principals'), column_names=f.get(
                'column_names', []), excluded_column_names=f.get('excluded_column_names', []))
            logger.debug(f"Start processing permissions for filter: {name}")
            current_permissions = self.lf_client.list_permissions(
                Resource={
                    "DataCellsFilter": {
                        "TableCatalogId": self.catalog_id,
                        "DatabaseName": self.database,
                        "TableName": self.table,
                        "Name": name,
                    }
                }
            )["PrincipalResourcePermissions"]

            current_principals = {
                p["Principal"]["DataLakePrincipalIdentifier"] for p in current_permissions}

            to_revoke = {
                p for p in current_principals if p not in filterConfig.principals}
            if to_revoke:
                self.lf_client.batch_revoke_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(
                        name, principal, idx) for idx, principal in enumerate(to_revoke)],
                )
                revoke_principals_msg = "\n".join(to_revoke)
                logger.debug(
                    f"Revoked permissions for filter {name} from principals:\n{revoke_principals_msg}")
            else:
                logger.debug(
                    f"No redundant permissions found for filter: {name}")

            to_add = {
                p for p in filterConfig.principals if p not in current_principals}
            if to_add:
                self.lf_client.batch_grant_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(
                        name, principal, idx) for idx, principal in enumerate(to_add)],
                )
                add_principals_msg = "\n".join(to_add)
                logger.debug(
                    f"Granted permissions for filter {name} to principals:\n{add_principals_msg}")
            else:
                logger.debug(f"No new permissions added for filter {name}")

            logger.debug(
                f"Permissions are set to be consistent with config for filter: {name}")

    def _permission_entry(self, filter_name: str, principal: str, idx: int):
        return {
            "Id": str(idx),
            "Principal": {"DataLakePrincipalIdentifier": principal},
            "Resource": {
                "DataCellsFilter": {
                    "TableCatalogId": self.catalog_id,
                    "DatabaseName": self.database,
                    "TableName": self.table,
                    "Name": filter_name,
                }
            },
            "Permissions": ["SELECT"],
            "PermissionsWithGrantOption": [],
        }
