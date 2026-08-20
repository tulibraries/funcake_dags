import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "funcake_dags" / "scripts" / "transform.sh"


class TransformScriptTest(unittest.TestCase):
    def test_skips_zero_byte_s3_objects(self):
        with tempfile.TemporaryDirectory() as tempdir:
            tempdir_path = Path(tempdir)
            bin_dir = tempdir_path / "bin"
            bin_dir.mkdir()
            uploads_log = tempdir_path / "uploads.log"
            uploads_log.write_text("", encoding="utf-8")
            saxon_jar = Path("/tmp/saxon/saxon-9.9.1-5.jar")
            saxon_jar.parent.mkdir(parents=True, exist_ok=True)
            saxon_jar.write_text("fake-jar", encoding="utf-8")

            self._write_executable(
                bin_dir / "aws",
                f"""#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = "s3api" ] && [ "$2" = "list-objects" ]; then
  printf '%s\\n' '{{"Contents":[{{"Key":"funcake_test/2021-03-23_17-25-10/new-updated-filtered/empty.xml","Size":0}}]}}'
elif [ "$1" = "s3" ] && [ "$2" = "presign" ]; then
  printf '%s\\n' 'file:///tmp/unused.xml'
elif [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  printf '%s\\n' "$3" >> "{uploads_log}"
else
  printf 'unexpected aws invocation: %s\\n' "$*" >&2
  exit 1
fi
""",
            )
            self._write_executable(
                bin_dir / "jq",
                """#!/usr/bin/env python3
import json
import sys

payload = json.load(sys.stdin)
for item in payload.get("Contents", []):
    print(f"{item['Key']}\\t{item['Size']}")
""",
            )
            self._write_executable(
                bin_dir / "java",
                """#!/usr/bin/env bash
printf 'java should not run for zero-byte inputs\\n' >&2
exit 1
""",
            )
            self._write_executable(
                bin_dir / "curl",
                """#!/usr/bin/env bash
printf 'curl should not run when Saxon jar already exists\\n' >&2
exit 1
""",
            )
            self._write_executable(
                bin_dir / "sha1sum",
                """#!/usr/bin/env bash
printf 'sha1sum should not run when Saxon jar already exists\\n' >&2
exit 1
""",
            )

            env = os.environ.copy()
            env.update(
                {
                    "BUCKET": "test-bucket",
                    "DAG_ID": "funcake_test",
                    "DAG_TS": "2021-03-23_17-25-10",
                    "DEST": "transformed",
                    "HOME": tempdir,
                    "PATH": f"{bin_dir}:{env['PATH']}",
                    "SCRIPTS_PATH": str(REPO_ROOT / "funcake_dags" / "scripts"),
                    "SOURCE": "new-updated-filtered",
                    "TMPDIR": tempdir,
                    "XSL_BRANCH": "main",
                    "XSL_FILENAME": "transforms/test.xsl",
                    "XSL_REPO": "tulibraries/aggregator_mdx",
                }
            )

            result = subprocess.run(
                [str(SCRIPT_PATH)],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Skipping empty source file", result.stdout)
            self.assertIn("Files transformed: 0", result.stdout)
            self.assertIn("Empty files skipped: 1", result.stdout)
            self.assertEqual(uploads_log.read_text(encoding="utf-8"), "")

    def _write_executable(self, path: Path, content: str):
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
