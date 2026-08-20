import os
import shutil
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
        result, uploads, transformed_files = self._run_script(
            '{"Contents":[{"Key":"funcake_test/2021-03-23_17-25-10/new-updated-filtered/empty.xml","Size":0}]}'
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Skipping empty source file", result.stdout)
        self.assertIn("Files transformed: 0", result.stdout)
        self.assertIn("Empty files skipped: 1", result.stdout)
        self.assertEqual(uploads, "")
        self.assertEqual(transformed_files, [])

    def test_fails_when_no_s3_objects_are_found(self):
        result, uploads, transformed_files = self._run_script('{"Contents":[]}')

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("No source files found", result.stderr)
        self.assertEqual(uploads, "")
        self.assertEqual(transformed_files, [])

    def test_processes_non_empty_objects_and_skips_empty_ones(self):
        listing = """{
            "Contents": [
                {"Key":"funcake_test/2021-03-23_17-25-10/new-updated-filtered/file1.xml","Size":100},
                {"Key":"funcake_test/2021-03-23_17-25-10/new-updated-filtered/empty.xml","Size":0},
                {"Key":"funcake_test/2021-03-23_17-25-10/new-updated-filtered/file2.xml","Size":200}
            ]
        }"""
        result, uploads, transformed_files = self._run_script(listing, java_mode="transform")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Files transformed: 2", result.stdout)
        self.assertIn("Empty files skipped: 1", result.stdout)
        self.assertEqual(
            uploads.strip().splitlines(),
            [
                "s3://test-bucket/funcake_test/2021-03-23_17-25-10/transformed/file1.xml",
                "s3://test-bucket/funcake_test/2021-03-23_17-25-10/transformed/file2.xml",
            ],
        )
        self.assertEqual(len(transformed_files), 2)

    def _run_script(self, listing_json: str, java_mode: str = "fail"):
        tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tempdir, ignore_errors=True)
        tempdir_path = Path(tempdir)
        bin_dir = tempdir_path / "bin"
        bin_dir.mkdir()
        uploads_log = tempdir_path / "uploads.log"
        uploads_log.write_text("", encoding="utf-8")
        saxon_jar = tempdir_path / "saxon.jar"
        saxon_jar.write_text("fake-jar", encoding="utf-8")

        self._write_executable(
            bin_dir / "aws",
            f"""#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = "s3api" ] && [ "$2" = "list-objects" ]; then
  cat <<'EOF'
{listing_json}
EOF
elif [ "$1" = "s3" ] && [ "$2" = "presign" ]; then
  printf '%s\\n' "file:///$3"
elif [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  printf '%s\\n' "$4" >> "{uploads_log}"
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

query = sys.argv[-1]
payload = json.load(sys.stdin)
contents = payload.get("Contents") or []
if query == "(.Contents // []) | length":
    print(len(contents))
elif query == "(.Contents // [])[] | [.Key, (.Size | tostring)] | @tsv":
    for item in contents:
        print(f"{item['Key']}\\t{item['Size']}")
else:
    raise SystemExit(f"unexpected jq query: {query}")
""",
        )
        self._write_executable(bin_dir / "java", self._java_script(java_mode))
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
                "SAXON_CP": str(saxon_jar),
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
            cwd=tempdir,
            env=env,
        )

        uploads = uploads_log.read_text(encoding="utf-8")
        transformed_files = sorted(path.name for path in tempdir_path.rglob("*.xml-transformed.xml"))
        return result, uploads, transformed_files

    def _java_script(self, java_mode: str):
        if java_mode == "fail":
            return """#!/usr/bin/env bash
printf 'java should not run for zero-byte inputs\\n' >&2
exit 1
"""

        return f"""#!/usr/bin/env bash
set -euo pipefail
output=""
for arg in "$@"; do
  case "$arg" in
    -o:*)
      output="${{arg#-o:}}"
      ;;
  esac
done

if [ -z "$output" ]; then
  printf 'missing output arg\\n' >&2
  exit 1
fi

mkdir -p "$(dirname "$output")"
if [[ "$output" == *"-transformed.xml" ]]; then
  cat <<'EOF' > "$output"
<root>
<oai_dc:dc xmlns:oai_dc="urn:oai_dc"/>
<dcterms:identifier>id</dcterms:identifier>
</root>
EOF
else
  cat <<'EOF' > "$output"
<?xml version="1.0"?>
<record/>
EOF
fi
"""

    def _write_executable(self, path: Path, content: str):
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
