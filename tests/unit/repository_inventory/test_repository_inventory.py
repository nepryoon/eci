"""Acceptance tests for SPEC-068's repository verification surface."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


class RepositoryInventoryTests(unittest.TestCase):
    def test_every_production_manifest_is_classified(self):
        inventory = REPO_ROOT / "scripts" / "module-inventory.sh"
        self.assertTrue(inventory.is_file(), "scripts/module-inventory.sh is missing")
        result = subprocess.run(
            ["bash", str(inventory), "--list"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        classified = set(result.stdout.splitlines())
        expected: set[str] = set()
        roots = ("services", "tools", "libs", "fakes")
        for manifest, kind in (
            ("go.mod", "go"),
            ("Cargo.toml", "rust"),
            ("pyproject.toml", "python"),
        ):
            for path in REPO_ROOT.glob(f"*/**/{manifest}"):
                relative = path.relative_to(REPO_ROOT)
                if relative.parts[0] in roots:
                    expected.add(f"{kind}\t{relative.parent.as_posix()}")
        self.assertSetEqual(classified, expected)

    def test_unit_targets_do_not_collect_docker_backed_tests(self):
        keycloak = (REPO_ROOT / "services/api-gateway/internal/authn/keycloak_integration_test.go").read_text()
        opa = (REPO_ROOT / "libs/go/eci/authz/opa_integration_test.go").read_text()
        orchestrator = (REPO_ROOT / "services/orchestrator/orchestrator/test_ask.py").read_text()
        self.assertTrue(keycloak.startswith("//go:build integration\n"))
        self.assertTrue(opa.startswith("//go:build integration\n"))
        self.assertIn("pytestmark = pytest.mark.integration", orchestrator)

    def test_taskfile_exposes_required_verification_targets(self):
        taskfile = (REPO_ROOT / "Taskfile.yml").read_text()
        for target in (
            "test:e2e:",
            "test:interop:",
            "test:fakes:",
            "test:security:",
            "verify:generated:",
            "verify:ci:",
        ):
            self.assertIn(target, taskfile, f"missing Taskfile target {target}")

    def test_ci_invokes_explicit_aggregate_surfaces(self):
        workflow = (REPO_ROOT / ".github/workflows/ci.yml").read_text()
        for command in (
            "task test:integration",
            "task test:e2e",
            "task test:interop",
            "task test:fakes",
            "task test:security",
            "task verify:generated",
            "task k8s:validate",
        ):
            self.assertIn(command, workflow, f"CI does not invoke {command}")


if __name__ == "__main__":
    unittest.main()
