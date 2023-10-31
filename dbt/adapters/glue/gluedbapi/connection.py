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
    _connect_lock = threading.Lock()

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

    def _build_session_id(
        self,
    ) -> str:
        """Builds a session ID based on configuration.

        `enable_session_per_model` is given precedence over `glue_session_reuse`.
        If `enable_session_per_model` is enabled, scope for session reuse (by same model, by other models, across dbt runs) is limited.
        """
        # ID can be max 255 bytes long
        iam_role_name = self._create_session_config["role_arn"].partition('/')[2] or self._create_session_config["role_arn"]
        iam_role_name = iam_role_name[:150]

        # Base id
        if self.credentials.glue_session_id:
            id = self.credentials.glue_session_id
        else:
            id = 'dbt-glue'

        if self.credentials.enable_session_per_model:
            if self._session_id_suffix:
                # Suffix would be the model name, already unique
                id = f'{id}__{self._session_id_suffix}'
            else:
                # Ensure no duplicate session id across models
                id = f'{id}__{iam_role_name}__{uuid.uuid4()}'
        elif not self.credentials.glue_session_reuse:
            # Multiple sessions could be created in parallel, ensure no duplicates
            id = f'{id}__{iam_role_name}__{uuid.uuid4()}'

        return id

    def _connect(self) -> None:
        """Creates a new session, if required, and waits until it's ready to use.

        If a session already exists with same id in FAILED/TIMEOUT/STOPPED state, deletes it and creates a new session.
        """
        logger.debug("GlueConnection _connect called")

        # Build a seSet session_id either from existing session, or build one

        if not self.session_id:
            # _session not found, build a session id and inject via a placeholder _session for state checking
            self._session = {
                "Session": {"Id": self._build_session_id()}
            }
        logger.debug(f'Using session id {self.session_id} to connect')

        try:
            _current_state = self.state
            logger.debug(f'Current session state is {_current_state}')
            # if current state is READY, nothing else to do

            if _current_state is None:
                # No session exists, create
                logger.debug(f'No session exists with id {self.session_id}, creating a new one')
                self._create_session(session_id=self.session_id)

                logger.debug(f'Session creation initiated for {self.session_id}, waiting it to be READY, currently in state {self.state}')
                self._session_waiter.wait(Id=self.session_id)

            elif _current_state in [
                GlueSessionState.PROVISIONING,
                GlueSessionState.STOPPING,
            ]:
                # Another action already in progress wait for success or failure
                # If fails, try to re-create
                try:
                    logger.debug(f'Waiting for session {self.session_id} to be READY, currently in state {self.state}')
                    self._session_waiter.wait(Id=self.session_id)
                except WaiterError as we:
                    if "Max attempts exceeded" in str(we):
                        raise TimeoutError(f"GlueSession took more than {self.credentials.session_provisioning_timeout_in_seconds} seconds to be ready")
                    else:
                        logger.debug(f"Session for {self.session_id} not usable, currently is in {self.state} state. Attempting to re-create...")
                        self._recreate_session(session_id=self.session_id)

                        logger.debug(f'Session recreation initiated for {self.session_id}, waiting it to be READY, currently in state {self.state}')
                        self._session_waiter.wait(Id=self.session_id)

            elif _current_state in [
                GlueSessionState.FAILED,
                GlueSessionState.STOPPED,
                GlueSessionState.TIMEOUT,
            ]:
                # Delete stale session and re-create
                self._recreate_session(session_id=self.session_id)

                logger.debug(f'Session recreation initiated for {self.session_id}, waiting it to be READY, currently in {self.state} state')
                self._session_waiter.wait(Id=self.session_id)

            # At this point, session should be in READY state
            # In case of any errors, would be hanlded as exception
            self._set_session_ready()
            return

        except WaiterError as we:
            # If it comes here, creation failed, do not re-try (loop)
            logger.exception(f'Connect failed to setup a session for {self.session_id}')
            raise dbterrors.FailedToConnectError(str(we))
        except Exception as e:
            # If it comes here, creation failed, do not re-try (loop)
            logger.exception(f'Error during connect for session {self.session_id}')
            raise dbterrors.FailedToConnectError(str(e))

    def _create_session(
        self,
        session_id: str,
    ) -> None:
        """Inititates the creation of a new Glue session from configuration."""
        logger.debug("GlueConnection _create_session called")

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

        self._session = self.client.create_session(
            Id=session_id,
            Role=self._create_session_config["role_arn"],
            DefaultArguments=args,
            Command={
                "Name": "glueetl",
                "PythonVersion": "3"
            },
            **additional_args
        )

        return

    def _recreate_session(
        self,
        session_id: str,
    ) -> None:
        """Deletes any existing session with session_id and creates a new one."""
        logger.debug("GlueConnection _recreate_session called")
        self.delete_session(session_id=session_id)
        logger.debug(f'Deleted session with id {session_id}')

        self._create_session(session_id=session_id)

        return

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

        if self.credentials.enable_session_per_model or not self.credentials.glue_session_reuse:
            # In these scenarios, the session id would be already unique for each thread/model running in parallel
            # there is no risk of ResourceAlreadyExists or InvalidInputException from Glue CreateSession API
            self._connect()
        else:
            # Only a single session would be created in this scenario, and reused across threads/models
            # Note: blocking lock, other threads/models running in parallel will wait until session is ready
            with self._connect_lock:
                self._connect()

        self._init_session()
        return GlueDictCursor(connection=self) if as_dict else GlueCursor(connection=self)

    def close_session(self):
        logger.debug("GlueConnection close_session called")
        if not self._session:
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
                logger.debug(f"session is already stopped or failed")
        except Exception as e:
            raise e

    @property
    def state(self) -> str:
        """Returns state of session with id `self.session_id`.

        `self.session_id` must already be set before invoking.
        """
        if self._state in [GlueSessionState.FAILED]:
            return self._state

        try:
            response = self.client.get_session(Id=self.session_id)
            session = response.get("Session", {})
            self._state = session.get("Status")
        except Exception as e:
            logger.debug(f"Error while checking state of session {self.session_id}")
            logger.debug(e)
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