import plistlib
import unittest

from delta_history.services import LABEL, render_service
from t3_delta.services import render_service as legacy_render


class GenericServiceTests(unittest.TestCase):
    def test_neutral_darwin_arguments_and_identity_do_not_change_legacy(self):
        arguments = ("darwin", "/opt/Agent Tools/delta-history", "/Users/user/config.json", "/Users/user/State & <history>")
        value = plistlib.loads(render_service(*arguments))
        self.assertEqual(value["Label"], LABEL)
        self.assertEqual(value["ProgramArguments"], [arguments[1], "--config", arguments[2], "--state-dir", arguments[3], "run"])
        self.assertEqual(plistlib.loads(legacy_render(*arguments))["Label"], "io.comcreate.t3-delta-recorder")

    def test_neutral_linux_queue_drainer_no_t3_command(self):
        result = render_service("linux", "/opt/delta-history", "/tmp/config %x.json", "/tmp/private-state").decode()
        self.assertIn("Description=Private Delta Agent History queue drainer", result)
        self.assertIn('ExecStart="/opt/delta-history" "--config" "/tmp/config %%x.json" "--state-dir" "/tmp/private-state" "run"', result)
        self.assertNotIn("t3", result)
        self.assertIn("UMask=0077", result)


if __name__ == "__main__":
    unittest.main()
