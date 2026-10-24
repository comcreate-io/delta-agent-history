import contextlib
import io
from pathlib import Path
import plistlib
import tempfile
import unittest

from t3_delta.services import LABEL, SERVICE_PATH, main, render_service, write_service


class ServiceTests(unittest.TestCase):
    def test_linux_literal_arguments_and_private_log(self):
        result = render_service("linux", '/opt/My "Tools"/t3-delta',
                                "/home/user/config $HOME %n; 'x'.json", "/home/user/State & <logs> %n $HOME").decode()
        self.assertIn('ExecStart="/opt/My \\"Tools\\"/t3-delta"', result)
        self.assertIn('"/home/user/config $$HOME %%n; \'x\'.json"', result)
        self.assertIn('StandardOutput=append:/home/user/State & <logs> %%n $HOME/recorder.log\n', result)
        self.assertIn("UMask=0077", result)
        self.assertIn("Restart=on-failure", result)
        self.assertNotIn("/bin/sh", result)

    def test_darwin_plist_roundtrips_without_shell_expansion(self):
        executable = '/Applications/My "Tools" & <dev>/t3-delta'
        config = "/Users/test/$HOME %x; config.json"
        state = "/Users/test/Library/Application Support/Delta & <state>"
        raw = render_service("darwin", executable, config, state)
        value = plistlib.loads(raw)
        self.assertIn(b"&amp;", raw)
        self.assertIn(b"&lt;", raw)
        self.assertEqual(value["Label"], LABEL)
        self.assertEqual(value["ProgramArguments"], [executable, "--config", config, "--state-dir", state, "run"])
        self.assertEqual(value["EnvironmentVariables"]["PATH"], SERVICE_PATH)
        self.assertEqual(value["Umask"], 0o077)
        self.assertEqual(value["KeepAlive"], {"SuccessfulExit": False})
        self.assertTrue(value["RunAtLoad"])
        self.assertEqual(value["StandardOutPath"], state + "/recorder.log")
        self.assertEqual(value["StandardErrorPath"], state + "/recorder.log")

    def test_invalid_paths_and_platform_fail_closed(self):
        for platform in ("linux", "darwin"):
            for value in ("relative", "~/relative", "/tmp/../escape", "/tmp/new\n[Service]", "/tmp/null\0", "/tmp/tab\t", "/tmp/del\x7f"):
                for index in range(3):
                    arguments = ["/opt/bin/t3-delta", "/tmp/config", "/tmp/state"]
                    arguments[index] = value
                    with self.subTest(platform=platform, value=value, index=index), self.assertRaises(ValueError):
                        render_service(platform, *arguments)
        with self.assertRaises(ValueError):
            render_service("windows", "/bin/tool", "/tmp/config", "/tmp/state")
        for value in ('/tmp/"ambiguous', "/tmp/back\\slash", "/tmp/trailing "):
            with self.assertRaises(ValueError):
                render_service("linux", "/bin/tool", "/tmp/config", value)

    def test_output_is_private_no_overwrite_or_symlink_following(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "service"
            write_service(output, b"first")
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                write_service(output, b"second")
            self.assertEqual(output.read_bytes(), b"first")
            write_service(output, b"second", force=True)
            self.assertEqual(output.read_bytes(), b"second")
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            symlink = Path(directory) / "link"
            symlink.symlink_to(output)
            for force in (True, False):
                with self.assertRaises(ValueError):
                    write_service(symlink, b"unsafe", force=force)
            self.assertEqual(output.read_bytes(), b"second")

    def test_cli_only_renders_requested_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "agent.plist"
            arguments = ["--platform", "darwin", "--executable", "/opt/tool", "--config", "/tmp/config", "--state-dir", "/tmp/state", "--output", str(output)]
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(arguments), 0)
                self.assertEqual(main(arguments), 2)
                self.assertEqual(main(arguments + ["--force"]), 0)
            self.assertEqual(list(Path(directory).iterdir()), [output])
            self.assertEqual(plistlib.loads(output.read_bytes())["Label"], LABEL)


if __name__ == "__main__":
    unittest.main()
