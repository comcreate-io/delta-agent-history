import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from delta_history.cli import MAX_JSON_DEPTH, MAX_LINE, drain_once, drain_run, main, parse_json, private_input, read_events


class GenericCliTests(unittest.TestCase):
    def test_jsonl_limits_fail_before_ingestion(self):
        self.assertEqual(read_events(io.BytesIO(b'{"ok":true}\n')), [{"ok": True}])
        for value in (b"", b"\n", b"[]\n", b'{"x":1,"x":2}\n', b'{"x":NaN}\n', b'{"x":Infinity}\n', b"{" + b"x" * MAX_LINE):
            with self.subTest(value=value[:30]), self.assertRaises(ValueError):
                read_events(io.BytesIO(value))
        with self.assertRaises(ValueError):
            read_events(io.StringIO('{}\n' * 101))

    def test_deep_json_is_a_redacted_input_error(self):
        with self.assertRaises(ValueError):
            parse_json('[' * 20000 + '0' + ']' * 20000)

    def test_json_depth_limit_counts_containers_not_quoted_text(self):
        value = parse_json('[' * MAX_JSON_DEPTH + '0' + ']' * MAX_JSON_DEPTH)
        for _ in range(MAX_JSON_DEPTH):
            value = value[0]
        self.assertEqual(value, 0)
        for value in ('[' * (MAX_JSON_DEPTH + 1) + '0' + ']' * (MAX_JSON_DEPTH + 1), '{"x":' * (MAX_JSON_DEPTH + 1) + '0' + '}' * (MAX_JSON_DEPTH + 1)):
            with self.assertRaises(ValueError):
                parse_json(value)
        text = '[{]}\\"' * 1000
        encoded = json.dumps({"text": text})
        self.assertEqual(parse_json(encoded), {"text": text})
        self.assertEqual(parse_json(encoded.encode()), {"text": text})

    def test_ingress_file_requires_private_regular_owned_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text('{"synthetic":true}\n')
            path.chmod(0o600)
            with private_input(path) as stream:
                self.assertEqual(read_events(stream), [{"synthetic": True}])
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                with private_input(path):
                    self.fail("Opened public file")
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                with private_input(link):
                    self.fail("Followed symlink")
            fifo = Path(directory) / "pipe"
            os.mkfifo(fifo, 0o600)
            with self.assertRaises(ValueError):
                with private_input(fifo):
                    self.fail("Opened nonregular file")

    def test_legacy_passthrough_preserves_original_arguments(self):
        with patch("t3_delta.cli.main", return_value=17) as legacy:
            self.assertEqual(main(["t3", "--config", "/tmp/old-config", "status"]), 17)
        legacy.assert_called_once_with(["--config", "/tmp/old-config", "status"])

    def test_init_offline_dispatch_and_paired_legacy_options(self):
        with patch("delta_history.configuration.initialize", return_value={"initialized": True}) as initialize:
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(["--config", "/tmp/config", "init", "--project", "/tmp/repo", "--source", "custom:desktop", "--delta-binary", "/bin/delta-app", "--accept-upload"]), 0)
            self.assertEqual(json.loads(output.getvalue()), {"initialized": True})
            self.assertEqual(initialize.call_args.kwargs["sources"], ["custom:desktop"])
            self.assertTrue(initialize.call_args.kwargs["accept_upload"])
        with patch("delta_history.configuration.initialize") as initialize:
            with contextlib.redirect_stderr(io.StringIO()) as error:
                self.assertEqual(main(["init", "--project", "/tmp/repo", "--source", "custom:desktop", "--legacy-t3-config", "/tmp/legacy"]), 2)
            initialize.assert_not_called()
            self.assertEqual(json.loads(error.getvalue())["error"], "ValueError")

    def test_local_ingest_dispatch_does_not_flush(self):
        with patch("delta_history.configuration.load", return_value={}), patch("delta_history.core.submit", return_value={"accepted": 1}) as submit, patch("delta_history.core.flush") as flush:
            with patch("sys.stdin", io.StringIO('{"synthetic":true}\n')), contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(["ingest"]), 0)
            self.assertEqual(json.loads(output.getvalue()), {"accepted": 1})
            self.assertEqual(submit.call_args.args[2], [{"synthetic": True}])
            flush.assert_not_called()

    def test_errors_are_redacted_and_not_written_to_stdout(self):
        secret = "never-echo-transcript-or-token"
        with patch("delta_history.configuration.load", side_effect=ValueError(secret)):
            with contextlib.redirect_stdout(io.StringIO()) as output, contextlib.redirect_stderr(io.StringIO()) as error:
                self.assertEqual(main(["status"]), 2)
            self.assertEqual(output.getvalue(), "")
            self.assertNotIn(secret, error.getvalue())

    def test_rejections_and_incomplete_drain_return_nonzero_with_metadata(self):
        with patch("delta_history.configuration.load", return_value={}):
            with patch("delta_history.core.submit", return_value={"accepted": 0, "rejected": 1, "halted": 1}):
                with patch("sys.stdin", io.StringIO('{"synthetic":true}\n')), contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(main(["ingest"]), 2)
                self.assertEqual(json.loads(output.getvalue())["rejected"], 1)
            for key in ("catching_up", "halted", "busy"):
                with patch("delta_history.core.flush", return_value={key: 1}), patch("delta_history.adapters.common.retry_pending", return_value={}):
                    with contextlib.redirect_stdout(io.StringIO()) as output:
                        self.assertEqual(main(["once"]), 2)
                    self.assertEqual(json.loads(output.getvalue())[key], 1)

    def test_run_retries_transient_failures_with_backoff_and_redacted_output(self):
        with patch("delta_history.cli.drain_once", side_effect=[RuntimeError("secret-output"), {"catching_up": 0}]) as flush:
            with patch("delta_history.cli.time.sleep", side_effect=[None, KeyboardInterrupt]) as sleep:
                with contextlib.redirect_stdout(io.StringIO()) as output, contextlib.redirect_stderr(io.StringIO()) as error:
                    with self.assertRaises(KeyboardInterrupt):
                        drain_run({"poll_seconds": 10}, Path("/tmp/unused"))
        self.assertEqual(flush.call_count, 2)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [20, 10])
        self.assertNotIn("secret-output", error.getvalue())
        self.assertEqual(json.loads(output.getvalue()), {"catching_up": 0})

    def test_drain_retries_frozen_adapters_before_core_delivery_and_exposes_holds(self):
        order = []
        report = {"sessions": 1, "submitted": 0, "halted": 1, "unavailable": 0, "errors": []}
        with patch("delta_history.adapters.common.retry_pending", side_effect=lambda *args: order.append("adapter") or report):
            with patch("delta_history.core.flush", side_effect=lambda *args: order.append("core") or {"halted": 0, "catching_up": 0}):
                value = drain_once({}, Path("/tmp/unused"))
        self.assertEqual(order, ["adapter", "core"])
        self.assertEqual(value["halted"], 1)
        self.assertEqual(value["adapters"], report)

    def test_adapter_retry_failure_does_not_block_existing_core_delivery(self):
        with patch("delta_history.adapters.common.retry_pending", side_effect=RuntimeError("private-value")):
            with patch("delta_history.core.flush", return_value={"delivered_sessions": 1}) as flush:
                value = drain_once({}, Path("/tmp/unused"))
        flush.assert_called_once()
        self.assertEqual(value["delivered_sessions"], 1)
        self.assertEqual(value["catching_up"], 1)
        self.assertEqual(value["adapters"]["errors"], ["RuntimeError"])
        self.assertNotIn("private-value", json.dumps(value))


if __name__ == "__main__":
    unittest.main()
