from dataclasses import dataclass
from dbt import exceptions as dbterrors
import boto3
from botocore.config import Config
from botocore.exceptions import WaiterError
from dbt.adapters.glue.gluedbapi.cursor import GlueCursor, GlueDictCursor
from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.gluedbapi.commons import GlueStatement
from dbt.adapters.glue.util import get_session_waiter
import threading
import uuid
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt_common.exceptions import ExecutableError

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

    def __init__(self, credentials: GlueCredentials, session_id_suffix: str = None, session_config_overrides = {}):
        self.credentials = credentials
        self._session_id_suffix = session_id_suffix
        self._session_config_overrides = session_config_overrides

        self._client = None
        self._session_waiter = None
        self._session = None
        self._state = None
        self._create_session_config = {}

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
            raise FailedToConnectError(str(we))
        except Exception as e:
            # If it comes here, creation failed, do not re-try (loop)
            logger.exception(f'Error during connect for session {self.session_id}')
            raise FailedToConnectError(str(e))

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
        logger.debug("GlueConnection _init_session called for session_id : " + self.session_id)
        statement = GlueStatement(client=self.client, session_id=self.session_id, code=SQLPROXY)
        try:
            logger.debug(f"Executing statement (SQLPROXY): {statement}")
            statement.execute()
        except Exception as e:
            logger.error(f"Error in GlueCursor (session_id={self.session_id}, SQLPROXY) execute: {e}")
            raise ExecutableError(str(e))

        statement = GlueStatement(client=self.client, session_id=self.session_id,
                                  code=f"spark.sql('use {self.credentials.database}')")
        try:
            logger.debug(f"Executing statement (use database) : {statement}")
            statement.execute()
        except Exception as e:
            logger.error(f"Error in GlueCursor (session_id={self.session_id}, SQLPROXY) execute: {e}")
            raise ExecutableError(str(e))

    @property
    def session_id(self):
        if not self._session:
            return None
        return self._session.get("Session", {}).get("Id", None)

    @property
    def use_arrow(self):
        return self.credentials.use_arrow

    @property
    def location(self):
        return self.credentials.location

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
                self._session_waiter = get_session_waiter(client=self._client, timeout=self.credentials.session_provisioning_timeout_in_seconds)
        return self._client

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
            request_origin = 'dbt-glue-'+self.credentials.role_arn.partition('/')[2] or self.credentials.role_arn
            request_origin = request_origin.replace('/', '')
            self.client.delete_session(
                Id=session_id,
                RequestOrigin=request_origin
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
import sys
import json
import base64
import urllib
import pandas as pd
import pyarrow.feather as feather
import boto3
import string
import random
from awsglue.utils import getResolvedOptions
params = []
if '--SESSION_ID' in sys.argv:
    params.append('SESSION_ID')
args = getResolvedOptions(sys.argv, params)
session_id = args.get("SESSION_ID", "unknown-session")

class SqlWrapper2:
    i = 0
    dfs = {}
    @classmethod
    def execute(cls,sql,output=True,use_arrow=False,location=""):
        if "dbt_next_query" in sql:
                response=None
                queries = sql.split("dbt_next_query")
                for q in queries:
                    if (len(q)):
                        if q==queries[-1]:
                            response=cls.execute(q,output=True)
                        else:
                            cls.execute(q,output=False)
                return response

        spark.conf.set("spark.sql.crossJoin.enabled", "true")
        df = spark.sql(sql)
        if len(df.schema.fields) == 0:
            dumped_empty_result = json.dumps({"type": "results","sql": sql,"schema": None, "results": None})
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

        description = [{"name":f.name, "type":str(f.dataType)} for f in df.schema]
        raw_results = {"type": "results", "rowcount": rowcount, "results": results, "description": description}

        if use_arrow:
            s3_client = boto3.client('s3')
            glue_client = boto3.client('glue')
            security_config_name = glue_client.get_session(Id=session_id)["Session"].get("SecurityConfiguration", None)
            o = urllib.parse.urlparse(location)
            result_bucket = o.netloc
            key = urllib.parse.unquote(o.path)[1:]
            if not key.endswith('/'):
                key = key + '/'
            suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            filename = f"{session_id}-{suffix}.feather"
            result_key = key + f"tmp-results/{filename}"
            pdf = pd.DataFrame.from_dict(raw_results, orient="index")
            feather.write_feather(pdf.transpose(), filename, "zstd")

            extra_args = {}
            if security_config_name:
                security_config = glue_client.get_security_configuration(Name=security_config_name)
                s3_encryption = security_config["SecurityConfiguration"]["EncryptionConfiguration"].get("S3Encryption", None)
                if s3_encryption and len(s3_encryption) > 0:
                    s3_encryption_mode = s3_encryption[0]["S3EncryptionMode"]
                    kms_key_arn = s3_encryption[0].get("KmsKeyArn", None)
                    if s3_encryption_mode == "SSE-S3":
                        extra_args["ServerSideEncryption"] = "AES256"
                    elif s3_encryption_mode == "SSE-KMS":
                        extra_args["ServerSideEncryption"] = "aws:kms"
                        extra_args["SSEKMSKeyId"] = kms_key_arn.split("/")[1]
            s3_client.upload_file(filename, result_bucket, result_key, ExtraArgs=extra_args)
            # Print and return only metadata instead of actual result data payload. The param use_arrow=True is always
            # used with output=True, and stdout is used to pass those values to Interactive Sessions API.
            del raw_results['results']
            raw_results['result_bucket'] = result_bucket
            raw_results['result_key'] = result_key
            raw_results['description'] = description
            dumped_results = json.dumps(raw_results, default=str)
            print(dumped_results)
            return dumped_results

        dumped_results = json.dumps(raw_results, default=str)
        if output:
            print(dumped_results)
        else:
            return dumped_results
"""
