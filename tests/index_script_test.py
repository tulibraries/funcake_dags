import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "funcake_dags" / "scripts" / "index.sh"


class IndexScriptTest(unittest.TestCase):
    def test_template_mode_without_data_fails_cleanly_when_s3_prefix_is_empty(self):
        result = self._run_script()

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("DATA: unbound variable", result.stdout + result.stderr)
        self.assertIn(
            "ERROR: no record sets found at s3://test-bucket/test-prefix/",
            result.stdout,
        )

    def test_index_mode_with_empty_data_reports_missing_record_sets(self):
        result = self._run_script(data="[]")

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("DATA: unbound variable", result.stdout + result.stderr)
        self.assertIn("ERROR: no record sets provided in DATA", result.stdout)

    def _run_script(self, data: str | None = None):
        tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tempdir, ignore_errors=True)
        tempdir_path = Path(tempdir)
        bin_dir = tempdir_path / "bin"
        bin_dir.mkdir()

        report_dir = tempdir_path / "dags" / "funcake_dags" / "scripts"
        report_dir.mkdir(parents=True)
        (report_dir / "publish_task_report.rb").write_text("", encoding="utf-8")

        self._write_executable(
            bin_dir / "git",
            """#!/usr/bin/env bash
set -euo pipefail
dest="${@: -1}"
mkdir -p "$dest/lib"
cat <<'EOF' > "$dest/lib/oai_index.rb"
"solr_writer.commit_on_close": true
EOF
""",
        )
        self._write_executable(bin_dir / "gem", "#!/usr/bin/env bash\nexit 0\n")
        self._write_executable(bin_dir / "bundle", "#!/usr/bin/env bash\nexit 0\n")
        self._write_executable(bin_dir / "aws", "#!/usr/bin/env bash\nexit 0\n")
        self._write_executable(
            bin_dir / "ruby",
            """#!/usr/bin/env bash
cat >/dev/null
printf "{ 'published': '0' }\\n"
""",
        )

        env = os.environ.copy()
        env.update(
            {
                "AIRFLOW_HOME": tempdir,
                "AIRFLOW_USER_HOME": tempdir,
                "BUCKET": "test-bucket",
                "FOLDER": "test-prefix/",
                "FUNCAKE_OAI_SOLR_URL": "http://example.test/solr/core",
                "HOME": tempdir,
                "INDEXER": "oai_index",
                "PATH": f"{bin_dir}:{env['PATH']}",
                "SOLR_AUTH_PASSWORD": "",
                "SOLR_AUTH_USER": "",
            }
        )
        if data is not None:
            env["DATA"] = data

        return subprocess.run(
            ["bash", str(SCRIPT_PATH)],
            check=False,
            capture_output=True,
            text=True,
            cwd=tempdir,
            env=env,
        )

    def _write_executable(self, path: Path, content: str):
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
