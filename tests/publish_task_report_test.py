import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "funcake_dags" / "scripts" / "publish_task_report.rb"


class PublishTaskReportTest(unittest.TestCase):
    def test_falls_back_to_traject_batch_totals(self):
        result = subprocess.run(
            ["ruby", str(SCRIPT_PATH)],
            input=(
                "finished Traject::Indexer#process: 2 records in 0.1 seconds\n"
                "finished Traject::Indexer#process: 3 records in 0.1 seconds\n"
            ),
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "{ 'published': '5' }")

    def test_prefers_solr_published_count_when_present(self):
        env = os.environ.copy()
        env["SOLR_PUBLISHED_COUNT"] = "4"

        result = subprocess.run(
            ["ruby", str(SCRIPT_PATH)],
            input="finished Traject::Indexer#process: 99 records in 0.1 seconds\n",
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "{ 'published': '4' }")
