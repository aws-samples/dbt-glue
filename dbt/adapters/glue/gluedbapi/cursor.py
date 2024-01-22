import uuid
import textwrap
import json
from dbt.contracts.connection import AdapterResponse
from dbt import exceptions as dbterrors
from dbt.adapters.glue.gluedbapi.commons import GlueStatement
from dbt.adapters.glue.util import get_pandas_dataframe_from_result_file
from dbt.events import AdapterLogger
from typing import Optional

logger = AdapterLogger("Glue")


class GlueCursorState:
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    AVAILABLE = "AVAILABLE"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class GlueCursor:
    def __init__(self, connection):
        self.name = str(uuid.uuid4())
        self._connection = connection
        self.state = None
        self._is_running = False
        self.statement_id = None
        self.code = None
        self.sql = None
        self.response = None
        self._result = None
        self._rowcount = -1
        self._description = None
        self._items = None
        self._columns = None
        self._closed = False

    @property
    def connection(self):
        return self._connection

    @property
    def rowcount(self):
        self.extract_properties_from_result()
        if self._rowcount != -1:
            return self._rowcount

    def _pre(self):
        self._it = None
        self._is_running = True
        self.response = None

    def _post(self):
        self._it = None
        self._is_running = False

    @classmethod
    def remove_comments_header(cls, sql: str):
        logger.debug("GlueCursor remove_comments_header called")
        comment_start = "/*"
        comment_end = "*/\n"
        if sql[0:len(comment_start)] == "/*":
            end = sql.index(comment_end)
            return sql[end + len(comment_end):]
        return sql

    @classmethod
    def add_end_space_if_single_quote(cls, sql: str):
        """ If query finishes with single quote ('),
        the execution of the query will fail. Ex: WHERE column='foo'
        """
        logger.debug("GlueCursor add_end_space_if_single_quote called")
        if sql.endswith("'"):
            return sql + " "
        return sql

    def execute(self, sql, bindings=None):
        logger.debug("GlueCursor execute called")
        if self.closed:
            raise Exception("CursorClosed")
        if self._is_running:
            raise dbterrors.InternalException("CursorAlreadyRunning")
        self.sql = GlueCursor.remove_comments_header(sql)
        self.sql = GlueCursor.add_end_space_if_single_quote(sql)

        self._pre()

        if "custom_glue_code_for_dbt_adapter" in self.sql:
            self.code = textwrap.dedent(self.sql.replace("custom_glue_code_for_dbt_adapter", ""))
        else:
            self.code = f"SqlWrapper2.execute('''{self.sql}''', use_arrow={self.connection.use_arrow}, location='{self.connection.location}')"

        self.statement = GlueStatement(
            client=self.connection.client,
            session_id=self.connection.session_id,
            code=self.code
        )

        logger.debug("client : " + self.code)
        try:
            response = self.statement.execute()
        except Exception as e:
            logger.exception(f"Error in GlueCursor (session_id={self.connection.session_id}) execute: {e}")
            raise dbterrors.ExecutableError

        logger.debug(f"response: {response}")
        self.state = response.get("Statement", {}).get("State", GlueCursorState.WAITING)

        if self.state == GlueCursorState.AVAILABLE:
            self._post()
            output = response.get("Statement", {}).get("Output", {})
            status = output.get("Status")
            logger.debug("status = " + status)
            if status == "ok":
                try:
                    self.response = json.loads(output.get("Data", {}).get("TextPlain", None).strip())
                except Exception as ex:
                    try:
                        chunks = output.get("Data", {}).get("TextPlain", None).strip().split('\n')
                        logger.debug(f"chunks: {chunks}")
                        self.response = json.loads(chunks[0])
                        logger.debug(f"response: {response}")
                    except Exception as ex:
                        logger.error("Could not parse " + json.loads(chunks[0]), ex)
                        self.state = GlueCursorState.ERROR
            else:
                error_message = f"Glue returned `{status}` for statement {self.statement_id} for code {self.code}, {output.get('ErrorName')}: {output.get('ErrorValue')}"
                if output.get('ErrorValue').find("is not a view"):
                    self.state = GlueCursorState.ERROR
                    logger.error(error_message)
                else:
                    logger.debug(error_message)
                    raise dbterrors.DbtDatabaseError(msg=error_message)

            result_bucket = self.response.get("result_bucket")
            result_key = self.response.get("result_key")
            if result_bucket and result_key:
                pdf = get_pandas_dataframe_from_result_file(result_bucket, result_key)
                self._result = pdf.to_dict('records')[0]

        if self.state == GlueCursorState.ERROR:
            self._post()
            output = response.get("Statement", {}).get("Output", {})
            error_message = f"Glue cursor returned `{output.get('Status')}` for statement {self.statement_id} for code {self.code}, {output.get('ErrorName')}: {output.get('ErrorValue')}"
            logger.debug(error_message)
            raise dbterrors.DbtDatabaseError(msg=error_message)

        if self.state in [GlueCursorState.CANCELLED, GlueCursorState.CANCELLING]:
            self._post()
            raise dbterrors.DbtDatabaseError(
                msg=f"Statement {self.connection.session_id}.{self.statement_id} cancelled.")

        logger.debug("GlueCursor execute successfully")
        return self.response

    def extract_properties_from_result(self):
        if self.response:
            if self.connection.use_arrow:
                result = self._result
            else:
                result = self.response
            if result:
                self._rowcount = result.get("rowcount")
                self._description = [[c["name"], c["type"]] for c in result.get("description", [])]
                self._items = result.get("results", [])
                self._columns = [column.get("name") for column in result.get("description", [])]

    @property
    def columns(self):
        self.extract_properties_from_result()
        if self._columns:
            return self._columns

    def fetchall(self):
        logger.debug("GlueCursor fetchall called")
        if self.closed:
            raise Exception("CursorClosed")

        if self.response:
            self.extract_properties_from_result()
            records = []
            logger.debug(f"GlueCursor fetchall results={self._columns}, use_arrow={self.connection.use_arrow}")
            for item in self._items:
                record = []
                for column in self._columns:
                    record.append(item.get("data", {}).get(column, None))
                records.append(record)
            return records

    def fetchmany(self, limit: Optional[int]):
        logger.debug("GlueCursor fetchmany called")
        if self.closed:
            raise Exception("CursorClosed")

        if self.response:
            self.extract_properties_from_result()
            records = []
            i = 0
            logger.debug(f"GlueCursor fetchmany results={self._columns}, use_arrow={self.connection.use_arrow}")
            for item in self._items:
                record = []
                for column in self._columns:
                    record.append(item.get("data", {}).get(column, None))
                if i < limit:
                    records.append(record)
                    i = i+1
            return records

    def fetchone(self):
        logger.debug("GlueCursor fetchone called")
        if self.closed:
            raise Exception("CursorClosed")
        if self.response:
            if not self._it:
                self._it = 0
            try:
                self.extract_properties_from_result()
                record = []
                logger.debug(f"GlueCursor fetchone results={self._columns}, use_arrow={self.connection.use_arrow}")
                item = self._items[self._it]
                for column in self._columns:
                    record.append(item.get("data", {}).get(column, None))
                self._it = self._it + 1
                return record
            except Exception:
                self._it = None
                return None

    def __iter__(self):
        return self

    def __next__(self):
        item = self.fetchone()
        if not item:
            raise StopIteration
        return item

    @property
    def description(self):
        logger.debug("GlueCursor description called")
        self.extract_properties_from_result()
        if self._description:
            return self._description

    def get_response(self) -> AdapterResponse:
        logger.debug("GlueCursor get_response called")
        if self.statement:
            r = self.statement._get_statement()
            return AdapterResponse(
                _message=f'r["State"]',
                code=self.sql,
                **r
            )

    def close(self):
        logger.debug("GlueCursor close called")
        if self._closed:
            raise Exception("CursorAlreadyClosed")
        self._closed = True

    @property
    def closed(self):
        return self._closed


class GlueDictCursor(GlueCursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def fetchone(self):
        logger.debug("GlueDictCursor fetchone called")
        item = super().fetchone()
        if not item:
            return None
        data = {}
        for i, c in enumerate(self.columns):
            data[c] = item[i]
        return data

    def fetchall(self):
        logger.debug("GlueDictCursor fetchall called")
        array_records = super().fetchall()
        dict_records = []
        for array_item in array_records:
            dict_record = {}
            for i, c in enumerate(self.columns):
                dict_record[c] = array_item[i]
            dict_records.append(dict_record)
        return dict_records
