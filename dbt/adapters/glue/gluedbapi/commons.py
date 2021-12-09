from waiter import wait

class GlueStatement:
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    AVAILABLE = "AVAILABLE"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"

    def __init__(self, client, session_id, code, timeout=300):
        self.client = client
        self.code = code
        self.session_id = session_id
        self.timeout = timeout
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
            if elasped > self.timeout:
                raise TimeoutError
            response = self._get_statement()
            state = response.get("Statement", {}).get("State", GlueStatement.WAITING)
            if state in [GlueStatement.AVAILABLE, GlueStatement.ERROR,GlueStatement.CANCELLING, GlueStatement.WAITING]:
                break
        return response
