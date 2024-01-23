from dataclasses import dataclass
from dbt import exceptions as dbterrors
import boto3
from botocore.config import Config
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
from botocore.exceptions import WaiterError
from dbt.adapters.glue.gluedbapi.cursor import GlueCursor, GlueDictCursor
from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.gluedbapi.commons import GlueStatement
import time
import threading
import uuid
from dbt.events import AdapterLogger

logger = AdapterLogger("Glue")


class GlueSessionState:
    READY = "READY"
    FAILED = "FAILED"
    PROVISIONING = "PROVISIONING"
    TIMEOUT = "TIMEOUT"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


@dataclass
class GlueConnection:
    _boto3_client_lock = threading.Lock()
    _create_session_config = {}

    def __init__(self, credentials: GlueCredentials, session_id_suffix: str = None, session_config_overrides = {}):
        self.credentials = credentials
        self._session_id_suffix = session_id_suffix
        self._session_config_overrides = session_config_overrides

        self._client = None
        self._session_waiter = None
        self._session = None
        self._state = None

        for key in self.credentials._connection_keys():
            self._create_session_config[key] = self._session_config_overrides.get(key) or getattr(self.credentials, key)

    def _connect(self):
        logger.debug("GlueConnection connect called")
        if not self.session_id:
            logger.debug("No session present, starting one")
            self._start_session()
        else:
            self._session = {
                "Session": {"Id": self.session_id}
            }
            logger.debug(f"Existing session {self.session_id} with status : {self.state}")
            try:
                self._session_waiter.wait(Id=self.session_id)
                self._set_session_ready()
                return self.session_id
            except WaiterError as e:
                if "Max attempts exceeded" in str(e):
                    raise TimeoutError(f"GlueSession took more than {self.credentials.session_provisioning_timeout_in_seconds} seconds to be ready")
                else:
                    logger.debug(f"session {self.session_id} is already stopped or failed")
                    self.delete_session(session_id=self.session_id)
                    self._session = self._start_session()
                    return self.session_id
            except Exception as e:
                raise e

        return self.session_id

    def _start_session(self):
        logger.debug("GlueConnection _start_session called")

        if self.credentials.glue_session_id:
            logger.debug(f"The existing session {self.credentials.glue_session_id} is used")
            try:
                self._session = self.client.get_session(
                    Id=self.credentials.glue_session_id,
                    RequestOrigin='string'
                )
            except Exception as e:
                logger.error(
                    f"Got an error when attempting to open a GlueSession : {e}"
                )
                raise dbterrors.FailedToConnectError(str(e))

            self._session_create_time = time.time()
        else:
            args = {
                "--enable-glue-datacatalog": "true"
            }

            if (self._create_session_config["default_arguments"] is not None):
                args.update(self._string_to_dict(self._create_session_config["default_arguments"].replace(' ', '')))

            if (self._create_session_config["extra_jars"] is not None):
                args["--extra-jars"] = f"{self._create_session_config['extra_jars']}"

            if (self._create_session_config["conf"] is not None):
                args["--conf"] = f"{self._create_session_config['conf']}"

            if (self._create_session_config["extra_py_files"] is not None):
                args["--extra-py-files"] = f"{self._create_session_config['extra_py_files']}"

            additional_args = {}
            additional_args["NumberOfWorkers"] = self._create_session_config["workers"]
            additional_args["WorkerType"] = self._create_session_config["worker_type"]
            additional_args["IdleTimeout"] = self._create_session_config["idle_timeout"]
            additional_args["Timeout"] = self._create_session_config["query_timeout_in_minutes"]
            additional_args["RequestOrigin"] = 'dbt-glue'

            if (self._create_session_config['glue_version'] is not None):
                additional_args["GlueVersion"] = f"{self._create_session_config['glue_version']}"

            if (self._create_session_config['security_configuration'] is not None):
                additional_args["SecurityConfiguration"] = f"{self._create_session_config['security_configuration']}"

            if (self._create_session_config["connections"] is not None):
                additional_args["Connections"] = {"Connections": list(set(self._create_session_config["connections"].split(',')))}

            if (self._create_session_config["tags"] is not None):
                additional_args["Tags"] = self._string_to_dict(self._create_session_config["tags"])

            if (self.credentials.datalake_formats is not None):
                args["--datalake-formats"] = f"{self.credentials.datalake_formats}"

            session_uuid = uuid.uuid4()
            session_uuid_str = str(session_uuid)
            session_prefix = self._create_session_config["role_arn"].partition('/')[2] or self._create_session_config["role_arn"]
            new_id = f"{session_prefix}-dbt-glue-{session_uuid_str}"

            if self._session_id_suffix:
                new_id = f"{new_id}-{self._session_id_suffix}"

            try:
                logger.debug(f"A new session {new_id} is created")
                self._session = self.client.create_session(
                    Id=new_id,
                    Role=self._create_session_config["role_arn"],
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
                raise dbterrors.FailedToConnectError(str(e))

            self._session_create_time = time.time()

    def _init_session(self):
        logger.debug("GlueConnection _init_session called for session_id : " + self.session_id)
        statement = GlueStatement(client=self.client, session_id=self.session_id, code=SQLPROXY)
        try:
            logger.debug(f"Executing statement (SQLPROXY): {statement}")
            statement.execute()
        except Exception as e:
            logger.error("Error in GlueCursor execute " + str(e))
            raise dbterrors.ExecutableError(str(e))

        statement = GlueStatement(client=self.client, session_id=self.session_id,
                                  code=f"spark.sql('use {self.credentials.database}')")
        try:
            logger.debug(f"Executing statement (use database) : {statement}")
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
        config = Config(
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            }
        )
        if not self._client:
            # reference on why lock is required - https://stackoverflow.com/a/61943955/6034432
            with self._boto3_client_lock:
                session = boto3.session.Session()
                self._client = session.client("glue", region_name=self.credentials.region, config=config)
                waiter_name = "SessionReady"
                waiter_model = self.configure_waiter_model()
                self._session_waiter = create_waiter_with_client(waiter_name, waiter_model, self._client)
        return self._client

    def configure_waiter_model(self):
        delay = 3
        max_attempts = self.credentials.session_provisioning_timeout_in_seconds / delay + 1
        waiter_config = {
            "version": 2,
            "waiters": {
                "SessionReady": {
                    "operation": "GetSession",
                    "delay": delay,
                    "maxAttempts": max_attempts,
                    "acceptors": [
                        {
                            "matcher": "path",
                            "expected": "READY",
                            "argument": "Session.Status",
                            "state": "success"
                        },
                        {
                            "matcher": "path",
                            "expected": "STOPPED",
                            "argument": "Session.Status",
                            "state": "failure"
                        },
                        {
                            "matcher": "path",
                            "expected": "TIMEOUT",
                            "argument": "Session.Status",
                            "state": "failure"
                        },
                        {
                            "matcher": "path",
                            "expected": "FAILED",
                            "argument": "Session.Status",
                            "state": "failure"
                        }
                    ]
                }
            }
        }
        return WaiterModel(waiter_config)

    def cancel_statement(self, statement_id):
        logger.debug("GlueConnection cancel_statement called")
        self.client.cancel_statement(
            SessionId=self.session_id,
            Id=statement_id
        )

    def cancel(self):
        logger.debug("GlueConnection cancel called")
        response = self.client.list_statements(SessionId=self.session_id)
        for statement in response["Statements"]:
            if statement["State"] in GlueSessionState.READY:
                self.cancel_statement(statement_id=statement["Id"])

    def delete_session(self, session_id):
        try:
            self.client.delete_session(
                Id=session_id,
                RequestOrigin='dbt-glue-'+self.credentials.role_arn.partition('/')[2] or self.credentials.role_arn
            )
        except Exception as e:
            logger.debug(f"delete session {session_id} error")
            raise e

    def close(self):
        if not self.credentials.enable_session_per_model:
            logger.debug("NotImplemented: close")
            return
        logger.debug("GlueConnection close called")
        self.close_session()

    @staticmethod
    def rollback():
        logger.debug("NotImplemented: rollback")

    def cursor(self, as_dict=False) -> GlueCursor:
        logger.debug("GlueConnection cursor called")
        self._connect()
        if self.state == GlueSessionState.READY:
            self._init_session()
            return GlueDictCursor(connection=self) if as_dict else GlueCursor(connection=self)
        elif self.session_id:
            try:
                logger.debug(f"[cursor waiting glue session state to ready for {self.session_id} in {self.state} state")
                self._session_waiter.wait(Id=self.session_id)
                self._init_session()
                return GlueDictCursor(connection=self) if as_dict else GlueCursor(connection=self)
            except WaiterError as e:
                if "Max attempts exceeded" in str(e):
                    raise TimeoutError(f"GlueSession took more than {self.credentials.session_provisioning_timeout_in_seconds} seconds to start")
                else:
                    raise ValueError(f"session {self.session_id} is already stopped or failed")
            except Exception as e:
                raise e
        else:
            raise ValueError("Failed to get cursor")

    def close_session(self):
        logger.debug("GlueConnection close_session called")
        if not self._session or not self.session_id:
            logger.debug("session is not set to close_session")
            return
        if self.credentials.glue_session_reuse:
            logger.debug(f"reuse session, do not stop_session for {self.session_id} in {self.state} state")
            return
        try:
            self._session_waiter.wait(Id=self.session_id)
            logger.debug(f"[calling stop_session for {self.session_id} in {self.state} state")
            self.client.stop_session(Id=self.session_id)
        except WaiterError as e:
            if "Max attempts exceeded" in str(e):
                raise e
            else:
                logger.debug(f"session {self.session_id} is already stopped or failed")
        except Exception as e:
            raise e

    @property
    def state(self):
        if self._state in [GlueSessionState.FAILED]:
            return self._state
        try:
            if not self.session_id:
                logger.debug(f"session is set defined")
                self._state = GlueSessionState.STOPPED
            else:
                response = self.client.get_session(Id=self.session_id)
                session = response.get("Session", {})
                self._state = session.get("Status")
        except Exception as e:
            logger.debug(f"get session state error session_id: {self._session_id_suffix}, {self.session_id}. Exception: {e}")
            self._state = GlueSessionState.STOPPED
        return self._state

    def _set_session_ready(self):
        try:
            response = self.client.get_session(Id=self.session_id)
            self._session = response
            self._session_create_time = response.get("Session", {}).get("CreatedOn")
        except Exception as e:
            logger.debug(f"set session ready error")
            raise e

    def _string_to_dict(self, value_to_convert):
        value_in_dictionary = {}
        for i in value_to_convert.split(","):
            value_in_dictionary[i.split("=")[0].strip('\'').replace("\"", "")] = i.split("=")[1].strip('"\'')
        return value_in_dictionary


SQLPROXY = """
import json
import base64
class SqlWrapper2:
    i = 0
    dfs = {}
    @classmethod
    def execute(cls,sql,output=True):
        sql = sql.replace('"', '')
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