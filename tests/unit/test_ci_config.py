"""CI workflow configuration policy tests.

Validates that GitHub Actions workflow files follow project conventions:
- Checkout steps use default refs in pull_request_target workflows
- AWS credentials are configured in the expected order
- Push-to-main jobs and workflow-level config remain stable
- Triggers, concurrency, and permissions blocks are consistent
"""

import copy
import os

import pytest
import yaml

WORKFLOWS_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, ".github", "workflows"
)

ALL_WORKFLOW_FILES = ["integration.yml", "python-models.yml", "s3-tables.yml"]

# PR-triggered jobs that should use default checkout ref
PR_JOBS = [
    ("integration.yml", "functional-tests-pr"),
    ("python-models.yml", "python-model-tests-pr"),
    ("s3-tables.yml", "test-s3-tables"),
]

FORK_REF = "${{ github.event.pull_request.head.sha }}"


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
    """Extract the 'on:' block (PyYAML parses bare 'on' as boolean True)."""
    return wf.get(True) or wf.get("on")


def _dict_diff_keys(expected, actual):
    """Return a summary of top-level keys that differ between two dicts."""
    diffs = []
    for k in sorted(set(list(expected.keys()) + list(actual.keys()))):
        if k not in expected:
            diffs.append(f"+{k} (added)")
        elif k not in actual:
            diffs.append(f"-{k} (removed)")
        elif expected[k] != actual[k]:
            diffs.append(f"~{k} (changed)")
    return diffs


# ── Baselines (captured once at import time) ───────────────────────────

def _snapshot_baselines():
    """Capture current workflow structure as the baseline for drift detection."""
    integration = _load_workflow("integration.yml")
    python_models = _load_workflow("python-models.yml")
    s3_tables = _load_workflow("s3-tables.yml")

    return {
        "integration_main_job": copy.deepcopy(
            integration["jobs"]["functional-tests-main"]
        ),
        "s3_tables_main_job": copy.deepcopy(
            s3_tables["jobs"]["s3-tables-tests-main"]
        ),
        "triggers": {
            "integration.yml": copy.deepcopy(_triggers(integration)),
            "python-models.yml": copy.deepcopy(_triggers(python_models)),
            "s3-tables.yml": copy.deepcopy(_triggers(s3_tables)),
        },
        "concurrency": {
            "integration.yml": copy.deepcopy(integration["concurrency"]),
            "python-models.yml": copy.deepcopy(python_models["concurrency"]),
            "s3-tables.yml": copy.deepcopy(s3_tables["concurrency"]),
        },
        "permissions": {
            "integration.yml": copy.deepcopy(integration["permissions"]),
            "python-models.yml": copy.deepcopy(python_models["permissions"]),
            "s3-tables.yml": copy.deepcopy(s3_tables["permissions"]),
        },
        "python_models_jobs": list(python_models["jobs"].keys()),
    }


_BASELINES = _snapshot_baselines()


# ── Checkout ref policy ────────────────────────────────────────────────


class TestCheckoutPolicy:
    """PR jobs using pull_request_target must use the default checkout ref."""

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_pr_job_uses_default_ref(self, workflow_file, job_name):
        """Checkout step should not override ref to fork HEAD."""
        wf = _load_workflow(workflow_file)
        job = wf["jobs"][job_name]
        idx = _find_step_index(job, "actions/checkout")
        assert idx != -1, f"No checkout step in {job_name}"

        ref = job["steps"][idx].get("with", {}).get("ref")
        assert ref != FORK_REF, (
            f"{workflow_file} → {job_name} should use default checkout ref"
        )

    @pytest.mark.parametrize("workflow_file,job_name", PR_JOBS)
    def test_credentials_before_fork_checkout(self, workflow_file, job_name):
        """AWS credentials must not follow a fork-code checkout."""
        wf = _load_workflow(workflow_file)
        job = wf["jobs"][job_name]

        checkout_idx = _find_step_index(job, "actions/checkout")
        creds_idx = _find_step_index(job, "configure-aws-credentials")
        assert checkout_idx != -1, f"No checkout step in {job_name}"
        assert creds_idx != -1, f"No credentials step in {job_name}"

        ref = job["steps"][checkout_idx].get("with", {}).get("ref")
        if ref == FORK_REF:
            assert creds_idx < checkout_idx, (
                f"{workflow_file} → {job_name}: credentials step should "
                f"precede fork-code checkout"
            )


# ── Workflow structure stability ───────────────────────────────────────


class TestMainJobStability:
    """Push-to-main jobs must not drift from their expected structure."""

    def test_functional_tests_main(self):
        wf = _load_workflow("integration.yml")
        actual = wf["jobs"]["functional-tests-main"]
        baseline = _BASELINES["integration_main_job"]
        assert actual == baseline, (
            "functional-tests-main structure drifted.\n"
            f"Diff: {_dict_diff_keys(baseline, actual)}"
        )

    def test_s3_tables_tests_main(self):
        wf = _load_workflow("s3-tables.yml")
        actual = wf["jobs"]["s3-tables-tests-main"]
        baseline = _BASELINES["s3_tables_main_job"]
        assert actual == baseline, (
            "s3-tables-tests-main structure drifted.\n"
            f"Diff: {_dict_diff_keys(baseline, actual)}"
        )

    def test_functional_tests_main_step_count(self):
        wf = _load_workflow("integration.yml")
        steps = wf["jobs"]["functional-tests-main"]["steps"]
        assert len(steps) == 8, f"Expected 8 steps, got {len(steps)}"

    def test_s3_tables_tests_main_step_count(self):
        wf = _load_workflow("s3-tables.yml")
        steps = wf["jobs"]["s3-tables-tests-main"]["steps"]
        assert len(steps) == 7, f"Expected 7 steps, got {len(steps)}"

    def test_functional_tests_main_has_credentials(self):
        wf = _load_workflow("integration.yml")
        job = wf["jobs"]["functional-tests-main"]
        assert _find_step_index(job, "configure-aws-credentials") != -1

    def test_s3_tables_tests_main_has_credentials(self):
        wf = _load_workflow("s3-tables.yml")
        job = wf["jobs"]["s3-tables-tests-main"]
        assert _find_step_index(job, "configure-aws-credentials") != -1

    def test_functional_tests_main_has_tox(self):
        wf = _load_workflow("integration.yml")
        job = wf["jobs"]["functional-tests-main"]
        assert any("tox" in s.get("run", "") for s in job["steps"])

    def test_s3_tables_tests_main_has_pytest(self):
        wf = _load_workflow("s3-tables.yml")
        job = wf["jobs"]["s3-tables-tests-main"]
        assert any("pytest" in s.get("run", "") for s in job["steps"])


# ── Workflow-level config stability ────────────────────────────────────


class TestWorkflowConfig:
    """Triggers, concurrency, and permissions must stay consistent."""

    @pytest.mark.parametrize("workflow_file", ALL_WORKFLOW_FILES)
    def test_triggers(self, workflow_file):
        wf = _load_workflow(workflow_file)
        assert _triggers(wf) == _BASELINES["triggers"][workflow_file]

    @pytest.mark.parametrize("workflow_file", ALL_WORKFLOW_FILES)
    def test_concurrency(self, workflow_file):
        wf = _load_workflow(workflow_file)
        assert wf["concurrency"] == _BASELINES["concurrency"][workflow_file]

    @pytest.mark.parametrize("workflow_file", ALL_WORKFLOW_FILES)
    def test_permissions(self, workflow_file):
        wf = _load_workflow(workflow_file)
        assert wf["permissions"] == _BASELINES["permissions"][workflow_file]


class TestPythonModelsJobList:
    """python-models.yml should only contain the expected jobs."""

    def test_no_push_job(self):
        wf = _load_workflow("python-models.yml")
        for name, job in wf["jobs"].items():
            assert "push" not in str(job.get("if", "")), (
                f"Unexpected push job: {name}"
            )

    def test_expected_jobs_only(self):
        wf = _load_workflow("python-models.yml")
        assert list(wf["jobs"].keys()) == _BASELINES["python_models_jobs"]
