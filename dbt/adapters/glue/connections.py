from contextlib import contextmanager
import agate
from typing import Any, List, Dict, Optional
from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtRuntimeError
import dbt
from dbt.adapters.glue.gluedbapi import GlueConnection, GlueCursor
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.events.contextvars import get_node_info
from dbt_common.clients.agate_helper import table_from_data_flat

logger = AdapterLogger("Glue")


class GlueSessionState:
    OPEN = "open"
    FAIL = "fail"


class ReturnCode:
    OK = "OK"


class GlueConnectionManager(SQLConnectionManager):
    TYPE = "glue"
    GLUE_CONNECTIONS_BY_KEY: Dict[str, GlueConnection] = {}

    @classmethod
    def data_type_code_to_name(cls, type_code: str) -> str:
        """
        Get the string representation of the data type from the metadata. Dbt performs a
        query to retrieve the types of the columns in the SQL query. Then these types are compared
        to the types in the contract config, simplified because they need to match what is returned
        by metadata (we are only interested in the broader type, without subtypes nor granularity).
        """
        return type_code.split("(")[0].split("<")[0].upper()

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: GlueCredentials = connection.credentials
        try:
            connection_args = {
                "credentials": credentials
            }

            if credentials.enable_session_per_model:
                key = get_node_info().get("unique_id", "no-node")
                connection_args['session_id_suffix'] = key

                session_config_overrides = {}
                for session_config in credentials._connection_keys():
                    if get_node_info().get("meta", {}).get(session_config):
                        session_config_overrides[session_config] = get_node_info().get("meta", {}).get(session_config)
                connection_args['session_config_overrides'] = session_config_overrides

            else:
                key = cls.get_thread_identifier()

            if not cls.GLUE_CONNECTIONS_BY_KEY.get(key):
                logger.debug(f"opening a new glue connection for thread : {key}")
                cls.GLUE_CONNECTIONS_BY_KEY[key]: GlueConnection = GlueConnection(**connection_args)
            connection.state = GlueSessionState.OPEN
            connection.handle = cls.GLUE_CONNECTIONS_BY_KEY[key]
            return connection
        except Exception as e:
            logger.error(
                f"Got an error when attempting to open a GlueSession : {e}"
            )
            connection.handle = None
            connection.state = GlueSessionState.FAIL
            raise FailedToConnectError(f"Got an error when attempting to open a GlueSessions: {e}")

    def cancel(self, connection):
        """ cancel ongoing queries """
        connection.handle.cancel()

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            self.release()
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise DbtRuntimeError(str(e))

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        """
        new to support dbt 0.19: this method replaces get_response
        """
        message = ReturnCode.OK
        return AdapterResponse(
            _message=message,
        )

    @classmethod
    def get_result_from_cursor(cls, cursor: GlueCursor, limit: Optional[int]) -> agate.Table:
        logger.debug("get_result_from_cursor called")
        data: List[Any] = []
        column_names: List[str] = []
        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            if limit:
                rows = cursor.fetchmany(limit)
            else:
                rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)
        return table_from_data_flat(
            data,
            column_names
        )

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def cleanup_all(self):
        logger.debug("cleanup called")
        for connection in self.GLUE_CONNECTIONS_BY_KEY.values():
            try:
                connection.close_session()
            except Exception as e:
                logger.exception(f"failed to close session : {e}")
                logger.debug("connection not yet initialized")
