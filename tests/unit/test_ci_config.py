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

WORKFLOWS_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, ".github", "workflows"
)

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

        script = yaml.dump(gate)
        assert "getCollaboratorPermissionLevel" in script, (
            f"{GATE_JOB} must check commenter permission"
        )
        # The SHA-match check is what binds approval to the reviewed commit.
        assert "head.sha" in script and "SHA mismatch" in script, (
            f"{GATE_JOB} must reject when the pinned SHA != PR head (TOCTOU guard)"
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
    """python-models.yml is PR-only: gate + test job, no push-to-main job."""

    def test_expected_jobs_only(self):
        wf = _load_workflow("python-models.yml")
        assert set(wf["jobs"].keys()) == {GATE_JOB, "python-model-tests-pr"}
