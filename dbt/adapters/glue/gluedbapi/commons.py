from waiter import wait
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Glue")


class GlueStatement:
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    AVAILABLE = "AVAILABLE"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"

    def __init__(self, client, session_id, code):
        self.client = client
        self.code = code
        self.session_id = session_id
        self._statement_id = None

    def _run_statement(self):
        if not self._statement_id:
            self._statement_id = self.client.run_statement(
                SessionId=self.session_id,
                Code=self.code
            )["Id"]
        return self._statement_id

    def _get_statement(self):
        return self.client.get_statement(
            SessionId=self.session_id,
            Id=self._statement_id
        )

    def execute(self):
        logger.debug(f"RunStatement (session_id={self.session_id}, statement_id={self._statement_id})")
        self._run_statement()
        for elasped in wait(1):
            response = self._get_statement()
            logger.debug(f"GetStatement (session_id={self.session_id}, statement_id={self._statement_id}) response: {response}")
            state = response.get("Statement", {}).get("State", GlueStatement.WAITING)

            if state in [GlueStatement.AVAILABLE, GlueStatement.ERROR,GlueStatement.CANCELLING, GlueStatement.TIMEOUT]:
                break
        return response
