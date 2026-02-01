"""Tests for workflow management functionality."""
import pytest
import json
import os

# Import callable functions from test helpers (extracts underlying functions from MCP tools)
from test_helpers import save_workflow_step, get_workflow, clear_workflow, get_audit_log


class TestWorkflowManagement:
    """Tests for workflow management tools."""

    @pytest.fixture(autouse=True)
    def setup_workflow(self, temp_dir):
        """Set up temporary workflow files."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
        import config as cfg

        # Override settings to use temp directory
        self.original_workflow = cfg.settings.workflow_file
        self.original_audit = cfg.settings.audit_log

        cfg.settings.workflow_file = os.path.join(temp_dir, "workflow.json")
        cfg.settings.audit_log = os.path.join(temp_dir, "audit.csv")

        # Initialize files
        with open(cfg.settings.workflow_file, "w") as f:
            json.dump({"version": "1.0", "steps": []}, f)
        with open(cfg.settings.audit_log, "w") as f:
            f.write("timestamp,user,action,impact\n")

        yield

        # Restore original settings
        cfg.settings.workflow_file = self.original_workflow
        cfg.settings.audit_log = self.original_audit

    def test_save_workflow_step(self):
        """Should save a workflow step."""
        result = json.loads(save_workflow_step(
            "Compare customers",
            "compare_hashes",
            '{"source_a": "a.csv", "source_b": "b.csv"}'
        ))

        assert "error" not in result
        assert result["status"] == "success"
        assert result["step_id"] >= 1  # Step ID should be positive

    def test_get_workflow(self):
        """Should retrieve saved workflow."""
        # Get initial count
        initial_workflow = json.loads(get_workflow())
        initial_count = len(initial_workflow.get("steps", []))

        # Add a step
        save_workflow_step("Step 1", "compare", '{}')

        result = json.loads(get_workflow())

        assert "error" not in result
        # Should have one more step than before
        assert len(result["steps"]) == initial_count + 1

    def test_clear_workflow(self):
        """Should clear all workflow steps."""
        # Add steps
        save_workflow_step("Step 1", "compare", '{}')
        save_workflow_step("Step 2", "fuzzy", '{}')

        # Clear
        result = json.loads(clear_workflow())
        assert result["status"] == "success"

        # Verify empty
        workflow = json.loads(get_workflow())
        assert len(workflow["steps"]) == 0

    def test_get_audit_log(self):
        """Should retrieve audit log entries."""
        # Create some audit entries
        save_workflow_step("Test", "test", '{}')

        result = json.loads(get_audit_log(limit=5))

        assert "error" not in result
        assert "entries" in result
