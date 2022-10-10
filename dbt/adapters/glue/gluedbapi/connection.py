from dataclasses import dataclass
from dbt import exceptions as dbterrors
import boto3
from waiter import wait
from dbt.adapters.glue.gluedbapi.cursor import GlueCursor, GlueDictCursor
from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.gluedbapi.commons import GlueStatement
import uuid
from dbt.events import AdapterLogger

logger = AdapterLogger("Glue")


class GlueSessionState:
    READY = "READY"
    FAILED = "FAILED"
    PROVISIONING = "PROVISIONING"
    RUNNING = "RUNNING"
    CLOSED = "CLOSED"


@dataclass
class GlueConnection:

    def __init__(self, credentials: GlueCredentials):
        self.credentials = credentials
        self._client = None
        self._session = None
        self._state = None

    def connect(self):
        logger.debug("GlueConnection connect called")
        if not self.credentials.session_id:
            logger.debug("No session present, starting one")
            self._start_session()
        else:
            self._session = {
                "Session": {"Id": self.credentials.session_id}
            }
            logger.debug("Existing session with status : " + self.state)
            if self.state == GlueSessionState.CLOSED:
                self._session = self._start_session()

        self._init_session()
        self.credentials.session_id = self.session_id
        return self.session_id

    def _start_session(self):
        logger.debug("GlueConnection _start_session called")

        args = {
            "--enable-glue-datacatalog": "true"
        }

        if (self.credentials.extra_jars is not None):
            args["--extra-jars"] = f"{self.credentials.extra_jars}"

        if (self.credentials.conf is not None):
            args["--conf"] = f"{self.credentials.conf}"

        if (self.credentials.extra_py_files is not None):
            args["--extra-py-files"] = f"{self.credentials.extra_py_files}"

        additional_args = {}
        additional_args["NumberOfWorkers"] = self.credentials.workers
        additional_args["WorkerType"] = self.credentials.worker_type
        additional_args["IdleTimeout"] = self.credentials.idle_timeout
        additional_args["Timeout"] = self.credentials.query_timeout_in_seconds
        additional_args["RequestOrigin"] = 'dbt-glue'
        
        if (self.credentials.glue_version is not None):
            additional_args["GlueVersion"] = f"{self.credentials.glue_version}"
        
        if (self.credentials.security_configuration is not None):
            additional_args["SecurityConfiguration"] = f"{self.credentials.security_configuration}"
        
        if (self.credentials.connections is not None):
            additional_args["Connections"] = {"Connections": list(set(self.credentials.connections.split(',')))}

        if (self.credentials.tags is not None):
            tags_dictionary = {}
            for i in self.credentials.tags.split(","):
                tags_dictionary[i.split("=")[0].strip('\'').replace("\"", "")] = i.split("=")[1].strip('"\'')
            additional_args["Tags"] = tags_dictionary

        session_uuid = uuid.uuid4()
        session_uuidStr = str(session_uuid)
        session_prefix = self.credentials.role_arn.partition('/')[2] or self.credentials.role_arn

        try:
            self._session = self.client.create_session(
                Id=f"{session_prefix}-dbt-glue-{session_uuidStr}",
                Role=self.credentials.role_arn,
                DefaultArguments=args,
                Command={
                    "Name": "glueetl",
                    "PythonVersion": "3"
                },
                **additional_args)
        except Exception as e:
            logger.error(
                f"Got an error when attempting to open a GlueSession : {e}"
            )
            raise dbterrors.FailedToConnectException(str(e))

        for elapsed in wait(1):
            if self.state == GlueSessionState.READY:
                return self._session
            if elapsed > self.credentials.session_provisioning_timeout_in_seconds:
                raise TimeoutError(f"GlueSession took more than {self.credentials.session_provisioning_timeout_in_seconds} seconds to start")

    def _init_session(self):
        logger.debug("GlueConnection _init_session called")
        logger.debug("GlueConnection session_id : " + self.session_id)
        statement = GlueStatement(client=self.client, session_id=self.session_id, code=SQLPROXY)
        try:
            statement.execute()
        except Exception as e:
            logger.error("Error in GlueCursor execute " + str(e))
            raise dbterrors.ExecutableError(str(e))

        statement = GlueStatement(client=self.client, session_id=self.session_id,
                                  code=f"spark.sql('use {self.credentials.database}')")
        try:
            statement.execute()
        except Exception as e:
            logger.error("Error in GlueCursor execute " + str(e))
            raise dbterrors.ExecutableError(str(e))

    @property
    def session_id(self):
        if not self._session:
            return None
        return self._session.get("Session", {}).get("Id", None)

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client("glue", region_name=self.credentials.region)
        return self._client

    def cancel_statement(self, statement_id):
        logger.debug("GlueConnection cancel_statement called")
        self.client.cancel_statement(
            SessionId=self.session_id,
            Id=statement_id
        )

    def cancel(self):
        logger.debug("GlueConnection cancel called")
        response = self.client.get_statements(SessionId=self.session_id)
        for statement in response["Statements"]:
            if statement["State"] in GlueSessionState.RUNNING:
                self.cancel_statement(statement_id=statement["Id"])

    def close(self):
        logger.debug("NotImplemented: close")

    @staticmethod
    def rollback():
        logger.debug("NotImplemented: rollback")

    def cursor(self, as_dict=False) -> GlueCursor:
        logger.debug("GlueConnection cursor called")
        return GlueDictCursor(connection=self) if as_dict else GlueCursor(connection=self)

    def close_session(self):
        if self.credentials.session_id:
            self.client.delete_session(Id=self.credentials.session_id)

    @property
    def state(self):
        if self._state in [GlueSessionState.FAILED, GlueSessionState.READY]:
            return self._state
        try:
            response = self.client.get_session(Id=self.session_id)
            session = response.get("Session", {})
            self._state = session.get("Status")
        except:
            self._state = GlueSessionState.CLOSED
        return self._state


SQLPROXY = """
import json
import base64
class SqlWrapper2:
    i = 0
    dfs = {}
    @classmethod
    def execute(cls,sql,output=True):
        if "dbt_next_query" in sql:
                response=None
                queries = sql.split("dbt_next_query")
                for q in queries:
                    if (len(q)):
                        if q==queries[-1]:
                            response=cls.execute(q,output=True)
                        else:
                            cls.execute(q,output=False)
                return  response   
                
        spark.conf.set("spark.sql.crossJoin.enabled", "true")    
        df = spark.sql(sql)
        if len(df.schema.fields) == 0:
            dumped_empty_result = json.dumps({"type" : "results","sql" : sql,"schema": None,"results": None})
            if output:
                print (dumped_empty_result)
            else:
                return dumped_empty_result
        results = []
        rowcount = df.count()
        for record in df.rdd.collect():
            d = {}
            for f in df.schema:
                d[f.name] = record[f.name]
            results.append(({"type": "record", "data": d}))
        dumped_results = json.dumps({"type": "results", "rowcount": rowcount,"results": results,"description": [{"name":f.name, "type":str(f.dataType)} for f in df.schema]},default=str)
        if output:
            print(dumped_results)
        else:
            return dumped_results
"""