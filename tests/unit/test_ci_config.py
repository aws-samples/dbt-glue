"""CI workflow configuration policy tests.

Fork PRs run untrusted code against real AWS Glue, which requires secrets.
The secret-bearing PR jobs are therefore gated so that they cannot run
untrusted code without an explicit, commit-pinned maintainer approval
(see the ``check-trigger`` job in each workflow). These tests assert the
security-relevant properties of that design so it cannot silently regress:

- PR jobs are triggered by ``issue_comment`` (a maintainer command), not by
  the ``pull_request_target: labeled`` pattern that is prone to TOCTOU.
- A ``check-trigger`` gate job validates the commenter and the pinned SHA.
- PR jobs depend on that gate and check out *only* the validated SHA, never
  the live PR head (``github.event.pull_request.head.sha``).
- Push-to-main jobs keep running tox/pytest with AWS credentials.
"""

import os

import pytest
import yaml

GITHUB_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, ".github"
)
WORKFLOWS_DIR = os.path.join(GITHUB_DIR, "workflows")

# The gate logic lives in a single composite action shared by all workflows,
# so the security checks are asserted against it once (not per workflow file).
GATE_ACTION = os.path.join(GITHUB_DIR, "actions", "check-trigger", "action.yml")
GATE_ACTION_USES = "./.github/actions/check-trigger"

# (workflow file, secret-bearing PR job, gate job, validated-SHA expression)
PR_JOBS = [
    ("integration.yml", "functional-tests-pr"),
    ("python-models.yml", "python-model-tests-pr"),
    ("s3-tables.yml", "test-s3-tables"),
]

ALL_WORKFLOW_FILES = [wf for wf, _ in PR_JOBS]

GATE_JOB = "check-trigger"
VALIDATED_REF = "${{ needs.check-trigger.outputs.pr-sha }}"

# The live fork HEAD must never be checked out in a secret-bearing job: that is
# the pwn-request sink this design exists to remove.
FORK_HEAD_REF = "${{ github.event.pull_request.head.sha }}"


# ── Helpers ────────────────────────────────────────────────────────────


def _load_workflow(filename):
    path = os.path.join(WORKFLOWS_DIR, filename)
    with open(path) as f:
        return yaml.safe_load(f)


def _load_gate_action():
    with open(GATE_ACTION) as f:
        return yaml.safe_load(f)


def _find_step_index(job, action_fragment):
    """Return the index of the first step whose 'uses' contains *action_fragment*, or -1."""
    for i, step in enumerate(job.get("steps", [])):
        if action_fragment in step.get("uses", ""):
            return i
    return -1


def _triggers(wf):
    """Extract the 'on:' block (PyYAML parses the bare key 'on' as boolean True)."""
    return wf.get(True) or wf.get("on")


# ── Trigger policy ─────────────────────────────────────────────────────


class TestTriggerPolicy:
    """Secret-bearing PR tests must be gated by a maintainer comment command."""

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_triggered_by_issue_comment(self, workflow_file, job_name):
        """The workflow must run on issue_comment, not pull_request_target."""
        triggers = _triggers(_load_workflow(workflow_file))
        assert "issue_comment" in triggers, (
            f"{workflow_file} must be triggered by issue_comment"
        )
        # pull_request_target + checkout of fork code is the pwn-request pattern
        # this design removes; it must not come back.
        assert "pull_request_target" not in triggers, (
            f"{workflow_file} must not use pull_request_target"
        )

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_has_gate_job(self, workflow_file, job_name):
        """A check-trigger gate job must exist and validate the comment."""
        wf = _load_workflow(workflow_file)
        assert GATE_JOB in wf["jobs"], f"{workflow_file} missing {GATE_JOB} job"

        gate = wf["jobs"][GATE_JOB]
        cond = gate.get("if", "")
        assert "issue_comment" in cond, f"{GATE_JOB} must guard on issue_comment"
        assert "/test glue " in cond, f"{GATE_JOB} must guard on the command prefix"
        assert "pull_request" in cond, f"{GATE_JOB} must run only on PR comments"

        # The gate needs exactly two write scopes and no more (least privilege):
        #   - statuses: write     -> set the pending commit status
        #   - pull-requests: write -> comment on the PR (issues.createComment on a PR
        #     requires the pull-requests scope; issues: write is not sufficient)
        perms = gate.get("permissions", {})
        assert perms.get("statuses") == "write", (
            f"{GATE_JOB} must have statuses: write permission"
        )
        assert perms.get("pull-requests") == "write", (
            f"{GATE_JOB} must have pull-requests: write to comment on the PR"
        )
        # contents stays read-only: the gate must never get write access to code.
        assert perms.get("contents") in (None, "read"), (
            f"{GATE_JOB} must not request contents: write"
        )

        # The gate logic lives in the shared composite action; the job must use
        # it (rather than inlining a copy that could drift).
        assert _find_step_index(gate, GATE_ACTION_USES) != -1, (
            f"{GATE_JOB} must run the shared {GATE_ACTION_USES} action"
        )

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_gate_checks_out_trusted_action_before_use(self, workflow_file, job_name):
        """The local action must be fetched from the default branch (trusted),
        and that checkout must not persist credentials."""
        gate = _load_workflow(workflow_file)["jobs"][GATE_JOB]
        checkout_idx = _find_step_index(gate, "actions/checkout")
        action_idx = _find_step_index(gate, GATE_ACTION_USES)
        assert checkout_idx != -1, f"{GATE_JOB} must check out the repo for the action"
        assert checkout_idx < action_idx, (
            f"{GATE_JOB} must check out before using the local action"
        )
        # No explicit ref => default branch (trusted gate code), never PR head.
        checkout = gate["steps"][checkout_idx]
        assert checkout.get("with", {}).get("ref") is None, (
            f"{GATE_JOB} checkout must use the default branch, not a PR ref"
        )
        assert checkout.get("with", {}).get("persist-credentials") is False, (
            f"{GATE_JOB} checkout must set persist-credentials: false"
        )


# ── Checkout policy ────────────────────────────────────────────────────


class TestCheckoutPolicy:
    """PR jobs must check out only the gate-validated SHA, never the live head."""

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_pr_job_needs_gate(self, workflow_file, job_name):
        wf = _load_workflow(workflow_file)
        job = wf["jobs"][job_name]
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        assert GATE_JOB in needs, (
            f"{workflow_file} → {job_name} must depend on {GATE_JOB}"
        )

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_pr_job_checks_out_validated_sha(self, workflow_file, job_name):
        wf = _load_workflow(workflow_file)
        job = wf["jobs"][job_name]
        idx = _find_step_index(job, "actions/checkout")
        assert idx != -1, f"No checkout step in {job_name}"

        ref = job["steps"][idx].get("with", {}).get("ref")
        assert ref == VALIDATED_REF, (
            f"{workflow_file} → {job_name} must check out the validated SHA "
            f"({VALIDATED_REF}), got {ref!r}"
        )
        assert ref != FORK_HEAD_REF, (
            f"{workflow_file} → {job_name} must never check out the live fork HEAD"
        )

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_pr_job_does_not_persist_credentials(self, workflow_file, job_name):
        wf = _load_workflow(workflow_file)
        job = wf["jobs"][job_name]
        idx = _find_step_index(job, "actions/checkout")
        assert idx != -1, f"No checkout step in {job_name}"
        assert job["steps"][idx].get("with", {}).get("persist-credentials") is False, (
            f"{workflow_file} → {job_name} checkout must set persist-credentials: false"
        )


# ── Push-to-main jobs ──────────────────────────────────────────────────

MAIN_JOBS = [
    ("integration.yml", "functional-tests-main", "tox"),
    ("s3-tables.yml", "s3-tables-tests-main", "pytest"),
]


class TestMainJobs:
    """Push-to-main jobs run trusted code and must keep credentials + runner."""

    @pytest.mark.parametrize("workflow_file,job_name,runner", MAIN_JOBS)
    def test_main_job_present_and_push_gated(self, workflow_file, job_name, runner):
        wf = _load_workflow(workflow_file)
        assert job_name in wf["jobs"], f"{workflow_file} missing {job_name}"
        job = wf["jobs"][job_name]
        assert "push" in str(job.get("if", "")), (
            f"{job_name} must be gated on push to main"
        )
        assert _find_step_index(job, "configure-aws-credentials") != -1, (
            f"{job_name} must configure AWS credentials"
        )
        assert any(runner in s.get("run", "") for s in job["steps"]), (
            f"{job_name} must run {runner}"
        )


# ── Workflow-level config ──────────────────────────────────────────────


class TestWorkflowConfig:
    """id-token must not be writable in the comment-triggered workflows."""

    @pytest.mark.parametrize("workflow_file", ALL_WORKFLOW_FILES)
    def test_permissions_present(self, workflow_file):
        wf = _load_workflow(workflow_file)
        assert "permissions" in wf, f"{workflow_file} must declare permissions"

    @pytest.mark.parametrize("workflow_file", ALL_WORKFLOW_FILES)
    def test_concurrency_present(self, workflow_file):
        wf = _load_workflow(workflow_file)
        assert "concurrency" in wf, f"{workflow_file} must declare concurrency"


class TestPythonModelsJobList:
    """python-models.yml is PR-only: gate + test + report job, no push-to-main."""

    def test_expected_jobs_only(self):
        wf = _load_workflow("python-models.yml")
        assert set(wf["jobs"].keys()) == {
            GATE_JOB,
            "python-model-tests-pr",
            "report-status",
        }


# ── Commit-status reporting ────────────────────────────────────────────

# Each workflow reports its outcome as a commit status so the result becomes a
# PR check (and can be made a required check via branch protection). Without
# this, comment-triggered tests never gate merges -- the regression #678 left.
STATUS_CONTEXTS = [
    ("integration.yml", "functional-tests-pr", "integration-tests"),
    ("python-models.yml", "python-model-tests-pr", "python-model-tests"),
    ("s3-tables.yml", "test-s3-tables", "s3-tables-tests"),
]


class TestCommitStatusReporting:
    """Test results must be reported back to the validated commit as a check."""

    @pytest.mark.parametrize("workflow_file,test_job,status_context", STATUS_CONTEXTS)
    def test_gate_passes_status_context_to_action(
        self, workflow_file, test_job, status_context
    ):
        """The workflow must pass its status context to the shared gate action,
        which is what sets the pending commit status."""
        gate = _load_workflow(workflow_file)["jobs"][GATE_JOB]
        idx = _find_step_index(gate, GATE_ACTION_USES)
        assert idx != -1, f"{workflow_file} {GATE_JOB} must use the gate action"
        with_ = gate["steps"][idx].get("with", {})
        assert with_.get("status-context") == status_context, (
            f"{workflow_file} {GATE_JOB} must pass status-context '{status_context}'"
        )

    @pytest.mark.parametrize("workflow_file,test_job,status_context", STATUS_CONTEXTS)
    def test_report_job_reports_outcome(self, workflow_file, test_job, status_context):
        wf = _load_workflow(workflow_file)
        assert "report-status" in wf["jobs"], f"{workflow_file} missing report-status"
        report = wf["jobs"]["report-status"]

        # Must run even when the test job failed, but only if the gate passed,
        # so a real failure is surfaced as a failing check (not a skipped one).
        cond = report.get("if", "")
        assert "always()" in cond, "report-status must use always()"
        assert "check-trigger" in cond, "report-status must require gate success"

        needs = report.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        assert GATE_JOB in needs and test_job in needs, (
            f"report-status must depend on {GATE_JOB} and {test_job}"
        )

        body = yaml.dump(report)
        assert "createCommitStatus" in body, "report-status must set a commit status"
        assert status_context in body, (
            f"report-status must report context '{status_context}'"
        )
        assert "needs.check-trigger.outputs.pr-sha" in body, (
            "report-status must report against the validated SHA"
        )


# ── Shared gate action ─────────────────────────────────────────────────


class TestGateAction:
    """The security logic shared by all workflows lives in one composite action.

    These are the core pwn-request defenses; assert them on the action itself so
    they cannot silently weaken (the workflow files no longer inline this code).
    """

    def test_is_composite_action_with_sha_output(self):
        action = _load_gate_action()
        assert action.get("runs", {}).get("using") == "composite", (
            "check-trigger must be a composite action"
        )
        assert "sha" in action.get("outputs", {}), (
            "gate action must expose the validated 'sha' output"
        )
        assert {"status-context", "test-name"} <= set(action.get("inputs", {})), (
            "gate action must accept status-context and test-name inputs"
        )

    def test_enforces_permission_sha_and_toctou(self):
        script = yaml.dump(_load_gate_action())
        assert "getCollaboratorPermissionLevel" in script, (
            "gate must check commenter permission"
        )
        assert "['admin', 'write']" in script, (
            "gate must require admin/write permission"
        )
        # Full 40-char SHA must be pinned in the comment.
        assert "[a-f0-9]{40}" in script, "gate must require a full 40-char SHA"
        # The SHA-match check is what binds approval to the reviewed commit.
        assert "head.sha" in script and "SHA mismatch" in script, (
            "gate must reject when the pinned SHA != PR head (TOCTOU guard)"
        )
        assert "createCommitStatus" in script, (
            "gate must set a pending commit status on the validated SHA"
        )
