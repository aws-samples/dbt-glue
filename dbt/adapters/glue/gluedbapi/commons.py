from waiter import wait

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
        self._run_statement()
        for elasped in wait(1):
            response = self._get_statement()
            state = response.get("Statement", {}).get("State", GlueStatement.WAITING)
            if state in [GlueStatement.AVAILABLE, GlueStatement.ERROR,GlueStatement.CANCELLING, GlueStatement.WAITING, GlueStatement.TIMEOUT]:
                break
        return response
