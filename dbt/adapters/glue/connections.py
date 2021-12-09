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
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.glue.gluedbapi import GlueConnection, GlueCursor


class GlueConnectionManager(SQLConnectionManager):
    TYPE = "glue"

    @contextmanager
    def exception_handler(self, sql: str, connection_name=""):
        try:
            yield
        except Exception as e:
            raise e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        try:
            cls._connection: GlueConnection = GlueConnection(credentials=credentials)
            cls._connection.connect()
            connection.state = "open"
            connection.handle = cls._connection
            return connection
        except Exception as e:
            logger.debug(
                f"Got an error when attempting to open a GlueSession : {e}"
            )
            connection.handle = None
            connection.state = "fail"
            raise FailedToConnectException(str(e))

    def cancel(self, connection):
        """ cancel ongoing queries """
        logger.debug("CANCEL " * 12)
        logger.debug("Cancelling queries")
        connection.handle.cancel()
        logger.debug("Queries canceled")

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release()
            raise RuntimeException(str(e))

    def get_response(cls, cursor) -> AdapterResponse:
        """
        new to support dbt 0.19: this method replaces get_response
        """
        return "OK"
        rows = cursor.rowcount
        return AdapterResponse(
            _message=message,
            rows_affected=0
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

