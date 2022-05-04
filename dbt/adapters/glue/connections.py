from contextlib import contextmanager
import agate
from typing import Any, List
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import (
    FailedToConnectException,
    RuntimeException
)
import dbt
from dbt.adapters.glue.gluedbapi import GlueConnection, GlueCursor
from dbt.events import AdapterLogger

logger = AdapterLogger("Glue")


class GlueSessionState:
    OPEN = "open"
    FAIL = "fail"

class ReturnCode:
    OK = "OK"

class GlueConnectionManager(SQLConnectionManager):
    TYPE = "glue"

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        try:
            cls._connection: GlueConnection = GlueConnection(credentials=credentials)
            cls._connection.connect()
            connection.state = GlueSessionState.OPEN
            connection.handle = cls._connection
            return connection
        except Exception as e:
            logger.error(
                f"Got an error when attempting to open a GlueSession : {e}"
            )
            connection.handle = None
            connection.state = GlueSessionState.FAIL
            raise FailedToConnectException(f"Got an error when attempting to open a GlueSessions: {e}")

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
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise RuntimeException(str(e))

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
    def get_result_from_cursor(cls, cursor: GlueCursor) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = cursor.columns
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(
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
        try:
            self._connection.close_session()
        except:
            logger.debug("connection not yet initialized")