"""Background-job admission control in slack_loop.sh.

Context (2026-09-03): every `last_*_epoch` counter in slack_loop.sh starts
at 0, so on boot ~27 background jobs fired in the same loop iteration, each
spawning a Python process that imports pandas before doing any work. On the
2 GB worker that blew the memory limit; the OOM killed the container
mid-run, it restarted, and all 27 fired again — six OOM kills in eight
minutes. The jobs were fine; launching them simultaneously was not.

_run_bg now starts a job only when fewer than BG_MAX_JOBS are running and
MemAvailable is above BG_MIN_AVAILABLE_MB, and queues it otherwise;
_bg_drain starts queued work once there is room. These tests pull the
helper block straight out of slack_loop.sh and exercise it with bash, so
they fail if the guarantees regress: nothing is dropped, nothing is
double-queued, the cap holds, and the memory guard defers rather than
drops.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SLACK_LOOP = REPO_ROOT / "slack_loop.sh"

HELPER_START = 'BG_PID_DIR="${BG_PID_DIR:-/tmp/slack_loop_bg}"'
HELPER_END = 'echo "" >> "$LOG"'


def _extract_helpers() -> str:
    src = SLACK_LOOP.read_text(encoding="utf-8")
    start = src.index(HELPER_START)
    end = src.index(HELPER_END, start)
    return src[start:end]


@unittest.skipIf(shutil.which("bash") is None, "bash not available")
class SlackLoopBackgroundAdmissionTests(unittest.TestCase):
    def _run(self, body: str, *, max_jobs: str = "2",
             min_available_mb: str = "1") -> str:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            helpers = tmpdir / "helpers.sh"
            helpers.write_text(_extract_helpers(), encoding="utf-8")
            script = tmpdir / "case.sh"
            script.write_text(
                textwrap.dedent(f"""
                set -uo pipefail
                export BG_PID_DIR="{tmpdir}/bg"
                export BG_MAX_JOBS="{max_jobs}"
                export BG_MIN_AVAILABLE_MB="{min_available_mb}"
                LOG="{tmpdir}/log"
                : > "$LOG"
                stamp() {{ echo TS; }}
                source "{helpers}"
                """) + textwrap.dedent(body),
                encoding="utf-8",
            )
            proc = subprocess.run(
                ["bash", str(script)], capture_output=True, text=True,
                timeout=120,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            return proc.stdout

    def test_runs_up_to_the_cap_then_queues(self) -> None:
        out = self._run("""
            _run_bg a "sleep 5"
            _run_bg b "sleep 5"
            echo "running=$(_bg_running_count)"
            _run_bg c "sleep 1"
            echo "after_third=$(_bg_running_count)"
            echo "queued=$(wc -l < "$BG_QUEUE" | tr -d ' ')"
        """)
        self.assertIn("running=2", out)
        self.assertIn("after_third=2", out)
        self.assertIn("queued=1", out)

    def test_same_job_is_not_queued_twice(self) -> None:
        out = self._run("""
            _run_bg a "sleep 5"
            _run_bg b "sleep 5"
            _run_bg c "sleep 1"
            _run_bg c "sleep 1"
            _run_bg d "sleep 1"
            echo "queued=$(wc -l < "$BG_QUEUE" | tr -d ' ')"
        """)
        self.assertIn("queued=2", out)

    def test_queued_jobs_run_once_capacity_frees_up(self) -> None:
        out = self._run("""
            _run_bg a "sleep 1"
            _run_bg b "sleep 1"
            _run_bg c "echo ran-c"
            _run_bg d "echo ran-d"
            sleep 3
            _bg_drain
            sleep 2
            echo "c_done=$(grep -c 'bg-c. done' "$LOG")"
            echo "d_done=$(grep -c 'bg-d. done' "$LOG")"
            echo "queue_len=$(wc -l < "$BG_QUEUE" | tr -d ' ')"
        """)
        self.assertIn("c_done=1", out)
        self.assertIn("d_done=1", out)
        self.assertIn("queue_len=0", out)

    def test_low_memory_defers_instead_of_dropping(self) -> None:
        out = self._run("""
            _run_bg e "echo ran-e"
            echo "started=$(_bg_running_count)"
            echo "queued=$(wc -l < "$BG_QUEUE" | tr -d ' ')"
            export BG_MIN_AVAILABLE_MB=1
            BG_MIN_AVAILABLE_MB=1
            _bg_drain
            sleep 2
            echo "e_done=$(grep -c 'bg-e. done' "$LOG")"
        """, min_available_mb="99999999")
        self.assertIn("started=0", out)
        self.assertIn("queued=1", out)
        self.assertIn("e_done=1", out)


if __name__ == "__main__":
    unittest.main()
