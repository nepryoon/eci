"""Regression tests for the maintained T5.6 runner boundary.

The recorded runner under ``artifacts/`` is immutable historical evidence.  New
executions must enter through ``scripts/run-t56-gpu.sh``, which deliberately
rejects version overrides instead of allowing evidence metadata to diverge from
the runtime selected by the archived script.
"""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHIVED_RUNNER = REPO_ROOT / "artifacts" / "t5.6" / "20260828T211053Z" / "run_t56_gpu.sh"
MAINTAINED_RUNNER = REPO_ROOT / "scripts" / "run-t56-gpu.sh"
ARCHIVED_SHA256 = "0122ce955c24b8bb93cecf1a39fa715a6611146c7dba4552930656f1a08acc05"


class TestT56RunnerBoundary(unittest.TestCase):
    def test_archived_runner_bytes_remain_immutable(self) -> None:
        digest = hashlib.sha256(ARCHIVED_RUNNER.read_bytes()).hexdigest()
        self.assertEqual(digest, ARCHIVED_SHA256)

    def test_archived_runner_is_not_directly_executable(self) -> None:
        mode = ARCHIVED_RUNNER.stat().st_mode
        self.assertEqual(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH), 0)

    def test_maintained_runner_rejects_unsupported_vllm_override_before_gpu_work(self) -> None:
        env = dict(os.environ)
        env["VLLM_VERSION"] = "0.29.0"
        result = subprocess.run(
            ["bash", str(MAINTAINED_RUNNER)],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 64)
        self.assertIn("unsupported VLLM_VERSION", result.stderr)
        self.assertIn("0.28.0", result.stderr)
        self.assertNotIn("nvidia-smi", result.stderr)


if __name__ == "__main__":
    unittest.main()
