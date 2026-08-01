"""Unit tests for scripts/guard.sh — SPEC-001 §7.

Builds throwaway git repos under a temp dir and exercises guard.sh against
synthetic commits: no protected touch, protected touch without ADR, protected
touch with ADR, plus the edge cases from SPEC-001 §4 (unresolved BASE_REF,
renamed file under contracts/).
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
GUARD_SCRIPT = REPO_ROOT / "scripts" / "guard.sh"


def run(cmd, cwd):
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"command failed: {cmd}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    return result


class GuardTestRepo:
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="guard-test-")
        run(["git", "init", "-q", "-b", "main"], cwd=self.dir)
        run(["git", "config", "user.email", "test@example.com"], cwd=self.dir)
        run(["git", "config", "user.name", "Guard Test"], cwd=self.dir)
        self.write("README.md", "baseline\n")
        self.commit_all("baseline")
        run(["git", "branch", "base"], cwd=self.dir)

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def write(self, relpath, content="content\n"):
        p = Path(self.dir) / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)

    def move_base_to_head(self):
        run(["git", "branch", "-f", "base", "HEAD"], cwd=self.dir)

    def rename(self, src, dst):
        run(["git", "mv", src, dst], cwd=self.dir)

    def commit_all(self, message):
        run(["git", "add", "-A"], cwd=self.dir)
        run(["git", "commit", "-q", "-m", message], cwd=self.dir)

    def run_guard(self, base_ref="base"):
        env = dict(os.environ)
        env["BASE_REF"] = base_ref
        return subprocess.run(["bash", str(GUARD_SCRIPT)], cwd=self.dir, env=env, capture_output=True, text=True)


class TestGuard(unittest.TestCase):
    def setUp(self):
        self.repo = GuardTestRepo()

    def tearDown(self):
        self.repo.cleanup()

    # Scenario 5: nessun tocco a contracts/ o docs/add/ -> passa sempre.
    def test_no_protected_touch_passes(self):
        self.repo.write("services/foo/main.go", "package main\n")
        self.repo.commit_all("add unrelated file")
        result = self.repo.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_no_protected_touch_passes_even_with_unrelated_adr(self):
        self.repo.write("services/foo/main.go", "package main\n")
        self.repo.write("docs/decisions/ADR-0002-unrelated.md", "# ADR\n")
        self.repo.commit_all("add unrelated file and an unrelated ADR")
        result = self.repo.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)

    # Scenario 3: tocco a contracts/ senza ADR -> fallisce con elenco file protetti.
    def test_protected_touch_without_adr_fails(self):
        self.repo.write("contracts/README.md", "docs\n")
        self.repo.commit_all("touch contracts without ADR")
        result = self.repo.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/decisions/", result.stderr)
        self.assertIn("contracts/README.md", result.stderr)

    def test_docs_add_touch_without_adr_fails(self):
        self.repo.write("docs/add/notes.md", "notes\n")
        self.repo.commit_all("touch docs/add without ADR")
        result = self.repo.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/add/notes.md", result.stderr)

    # Scenario 4: stessa PR ma con ADR aggiunto nello stesso diff -> passa.
    def test_protected_touch_with_adr_passes(self):
        self.repo.write("contracts/README.md", "docs\n")
        self.repo.write("docs/decisions/ADR-0001-test.md", "# ADR\n")
        self.repo.commit_all("touch contracts with ADR")
        result = self.repo.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)

    # §4: BASE_REF non risolvibile -> fallisce esplicitamente, non "passa" per errore.
    def test_base_ref_unresolvable_fails_explicitly(self):
        self.repo.write("services/foo/main.go", "package main\n")
        self.repo.commit_all("add unrelated file")
        result = self.repo.run_guard(base_ref="origin/main")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fetch", result.stderr.lower())

    # §4: rename sotto contracts/ (R100 old new) deve essere trattato come tocco a contracts/.
    def test_renamed_file_under_contracts_detected(self):
        self.repo.write("contracts/old-name.md", "docs\n")
        self.repo.commit_all("add contracts file")
        self.repo.move_base_to_head()
        self.repo.rename("contracts/old-name.md", "contracts/new-name.md")
        self.repo.commit_all("rename contracts file")
        result = self.repo.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("contracts/", result.stderr)


if __name__ == "__main__":
    unittest.main()
