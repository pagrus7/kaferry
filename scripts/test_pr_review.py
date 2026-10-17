"""PR review boundaries: untrusted snapshots and incomplete results cannot approve."""

import json
import os
import signal
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import pr_review as review


SHA = "a" * 40


def result(head=SHA, **changes):
    return {"head": head, "status": "complete", "summary": "Checked the changed behavior.",
            "reviewed_files": ["code.go"], "findings": [], **changes}


class TargetTest(unittest.TestCase):
    def test_fork_fallback_and_stale_runs(self):
        # Given a fork run with no PR list and two associated PRs, one updated.
        event = {"workflow_run": {"event": "pull_request", "conclusion": "success", "head_sha": SHA, "pull_requests": []}}
        def api(path):
            if "/commits/" in path:
                return [{"number": 1}, {"number": 2}]
            return {"state": "open", "base": {"repo": {"full_name": "owner/repo"}, "sha": "b" * 40},
                    "head": {"sha": SHA if path.endswith("/1") else "c" * 40}}
        # When selecting current PR commits.
        with patch.object(review, "gh", side_effect=api):
            rows = review.targets(event, "workflow_run", "owner/repo")
        # Then both reviewers run only for the exact, still-current PR commit.
        self.assertEqual([(r["number"], r["role"], r["head"]) for r in rows], [(1, "docs", SHA), (1, "test", SHA)])

    def test_non_pr_cancelled_and_invalid_manual_runs_make_no_api_calls(self):
        # Given workflow events that should not start paid reviews.
        for source, conclusion in [("push", "success"), ("pull_request", "cancelled")]:
            event = {"workflow_run": {"event": source, "conclusion": conclusion}}
            # When selecting work, then no provider or GitHub query is needed.
            with patch.object(review, "gh") as api:
                self.assertEqual(review.targets(event, "workflow_run", "owner/repo"), [])
                api.assert_not_called()
        with self.assertRaises(ValueError):
            review.targets({"inputs": {"pr_number": "1; command"}}, "workflow_dispatch", "owner/repo")


class ReviewTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Trusted contributor policy")
        agents = self.root / ".claude/agents"
        agents.mkdir(parents=True)
        for role in review.ROLES:
            (agents / f"{role}-reviewer.md").write_text("---\nname: reviewer\n---\nTrusted reviewer instructions")
        (self.root / "code.go").write_text("before\n")
        self.base = self.commit()
        (self.root / "code.go").write_text("after\n")
        (self.root / "AGENTS.md").write_text("Untrusted replacement instructions")
        (self.root / ".claude/settings.json").write_text('{"hooks":{"SessionStart":[{"command":"do not execute"}]}}')
        self.head = self.commit()
        self.git("checkout", "-q", self.base)
        self.git("remote", "add", "origin", str(self.root))
        self.output = Path(self.temp.name) / "report"

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, stderr=subprocess.PIPE).decode().strip()

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_reviews_data_with_trusted_policy_and_restricted_tools(self):
        # Given different base/head content and PR-controlled Claude configuration.
        def agent(args, directory):
            self.assertEqual((directory / "base/code.go").read_text(), "before\n")
            self.assertEqual((directory / "head/code.go").read_text(), "after\n")
            self.assertTrue((directory / "head/.claude/settings.json").is_file())
            self.assertIn("+after", (directory / "review.patch").read_text())
            system = args[args.index("--system-prompt") + 1]
            self.assertIn("Trusted contributor policy", system)
            self.assertNotIn("Untrusted replacement instructions", system)
            for flag in ["--bare", "--restricted", "--strict-mcp-config", "--disable-slash-commands", "--max-budget-usd"]:
                self.assertIn(flag, args)
            self.assertEqual(args[args.index("--tools") + 1], "Read,Grep,Glob")
            return result(self.head)
        # When running a review through snapshot preparation and reporting.
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "fixture"}), patch.object(review, "invoke", side_effect=agent):
            code = review.review(self.root, "docs", 1, self.base, self.head, self.output)
        # Then the exact commit has a complete, public report without changing the checkout.
        self.assertEqual(code, 0)
        self.assertEqual(json.loads((self.output / "review.json").read_text())["head"], self.head)
        self.assertEqual(self.git("status", "--porcelain"), "")

    def test_missing_credentials_never_invoke_the_agent(self):
        # Given no API key.
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}), patch.object(review, "invoke") as agent:
            # When reviewing, then no request is made and the result is explicitly incomplete.
            self.assertEqual(review.review(self.root, "test", 1, self.base, self.head, self.output), 1)
            agent.assert_not_called()
        report = json.loads((self.output / "review.json").read_text())
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("ANTHROPIC_API_KEY", report["summary"])

    def test_incomplete_stale_and_malformed_results_never_pass(self):
        for report in [result(self.head, status="incomplete"), result("c" * 40), None,
                       result(self.head, reviewed_files=[]), result(self.head, reviewed_files=["../outside"])]:
            with self.subTest(report=report):
                # Given an interrupted, stale or unusable agent result.
                with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "fixture"}), patch.object(review, "invoke", return_value=report):
                    # When publishing it, then it cannot become an approval.
                    self.assertEqual(review.review(self.root, "test", 1, self.base, self.head, self.output), 1)
                self.assertEqual(json.loads((self.output / "review.json").read_text())["status"], "incomplete")

    def test_snapshots_reject_symlinks_outside_the_review_directory(self):
        # Given a tracked link pointing outside the repository.
        (self.root / "escape").symlink_to("../../outside")
        head = self.commit()
        # When extracting untrusted source, then it cannot escape the snapshot.
        with self.assertRaises(tarfile.FilterError):
            review.snapshot(self.root, head, Path(self.temp.name) / "snapshot")
        self.assertFalse((Path(self.temp.name) / "outside").exists())

    def test_snapshot_limit_stops_reading_and_terminates_the_archive_process(self):
        # Given a snapshot stream that exceeds the configured bound.
        with patch.object(review, "SNAPSHOT_LIMIT", 1024), patch.object(review.subprocess, "Popen") as child:
            child.return_value.stdout.read.side_effect = [b"a" * 1024, b"b" * 1024, b"unread"]
            child.return_value.poll.return_value = None
            # When the limit is reached, then extraction stops before buffering the whole stream.
            with self.assertRaises(review.ReviewError):
                review.snapshot(self.root, self.head, Path(self.temp.name) / "snapshot")
            self.assertEqual(child.return_value.stdout.read.call_count, 2)
            child.return_value.kill.assert_called_once()


class ProviderAndCheckTest(unittest.TestCase):
    def test_cancellation_stops_and_reaps_the_owned_process_group(self):
        # Given a running Claude process when the workflow receives SIGTERM.
        previous = signal.getsignal(signal.SIGTERM)
        def cancel(**_kwargs):
            os.kill(os.getpid(), signal.SIGTERM)
        with patch.object(review.subprocess, "Popen") as child, patch.object(review.os, "killpg") as stop:
            child.return_value.communicate.side_effect = cancel
            # When cancellation interrupts the request.
            with self.assertRaises(review.ReviewError):
                review.invoke(["claude"], Path("."))
            # Then the paid process is stopped and reaped, and the caller's signal handler is restored.
            stop.assert_called_once_with(child.return_value.pid, signal.SIGKILL)
            child.return_value.wait.assert_called_once()
            self.assertTrue(child.call_args.kwargs["start_new_session"])
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_successful_provider_envelope_returns_its_structured_report(self):
        # Given a completed CLI call containing a structured review.
        report = result()
        with patch.object(review.subprocess, "Popen") as child:
            child.return_value.returncode = 0
            child.return_value.communicate.return_value = json.dumps({"subtype": "success", "is_error": False, "structured_output": report}), ""
            # When collecting the response, then its report is preserved for validation.
            self.assertEqual(review.invoke(["claude"], Path(".")), report)

    def test_provider_errors_never_expose_raw_output(self):
        for code, text in [(1, "secret-value"), (0, '{"subtype":"success","is_error":true,"result":"secret-value"}'),
                           (0, json.dumps({"subtype": "error_max_turns", "is_error": True, "structured_output": result()}))]:
            # Given a CLI error, including the provider's quota-error envelope.
            with patch.dict(os.environ, {"GH_TOKEN": "github-secret", "GITHUB_TOKEN": "github-secret"}), patch.object(review.subprocess, "Popen") as child:
                child.return_value.returncode = code
                child.return_value.communicate.return_value = text, "secret-value"
                # When collecting it, then only a fixed safe error is returned.
                with self.assertRaises(review.ReviewError) as error:
                    review.invoke(["claude"], Path("."))
                self.assertNotIn("secret-value", str(error.exception))
                env = child.call_args.kwargs["env"]
                self.assertNotIn("GH_TOKEN", env)
                self.assertNotIn("GITHUB_TOKEN", env)

    def test_pr_check_is_attached_to_the_requested_commit(self):
        # Given a PR head and the current Actions run.
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "123"}), patch.object(review, "gh", return_value={"id": 9}) as api:
            # When creating a review check, then it names that exact commit and links its run.
            self.assertEqual(review.check_start("owner/repo", "docs", SHA), 9)
            payload = api.call_args.args[1]
            self.assertEqual(payload["head_sha"], SHA)
            self.assertEqual(payload["name"], "Docs review")
            self.assertEqual(payload["details_url"], "https://github.com/owner/repo/actions/runs/123")

    def test_pr_checks_distinguish_findings_from_incomplete_reviews(self):
        for status, findings, expected in [("complete", [], "success"), ("complete", [{}], "neutral"), ("incomplete", [], "failure")]:
            with tempfile.TemporaryDirectory() as temp:
                output = Path(temp)
                (output / "review.json").write_text(json.dumps({"status": status, "findings": findings}))
                (output / "review.md").write_text("Report")
                # Given a completed review with/without findings, or an incomplete review.
                with patch.object(review.subprocess, "run") as api:
                    # When updating the PR check, then findings remain advisory and missing work never passes.
                    review.check_finish("owner/repo", 1, output)
                    self.assertEqual(json.loads(api.call_args.kwargs["input"])["conclusion"], expected)

    def test_interrupted_report_files_cannot_leave_a_successful_check(self):
        # Given a process interrupted between writing its JSON and Markdown reports.
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            (output / "review.json").write_text(json.dumps({"status": "complete", "findings": []}))
            with patch.object(review.subprocess, "run") as api:
                # When the always-run finalizer executes, then it publishes failure.
                review.check_finish("owner/repo", 1, output)
                self.assertEqual(json.loads(api.call_args.kwargs["input"])["conclusion"], "failure")


if __name__ == "__main__":
    unittest.main()
