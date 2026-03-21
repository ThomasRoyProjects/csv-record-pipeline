import tempfile
import unittest
from pathlib import Path

from core.jobs import build_runtime_config, load_job
from pipeline_runner import execute_runtime


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class DemoJobsIntegrationTests(unittest.TestCase):
    def test_demo_match_job_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "jobs" / "demo_match_job.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime["outputs"]["records"]["base_dir"] = str(Path(tmpdir) / "demo_match")
            result = execute_runtime(runtime)

            summary = result["summary"]
            counts = summary["counts"]

            self.assertEqual(summary["workflow"], "match_records_to_reference")
            self.assertEqual(counts["total_records"], 1000)
            self.assertEqual(counts["matched_records"], 600)
            self.assertEqual(counts["unmatched_records"], 350)
            self.assertTrue(Path(result["summary_path"]).exists())

    def test_demo_enrich_profile_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "profiles" / "demo_enrich.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime["outputs"]["records"]["base_dir"] = str(Path(tmpdir) / "demo_enrich")
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "enrich_records_from_reference")
            output_path = Path(summary["outputs"]["enriched_records"])
            self.assertTrue(output_path.exists())
            output_text = output_path.read_text()
            self.assertIn("reference_segment", output_text.splitlines()[0])

    def test_demo_extract_profile_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "profiles" / "demo_extract.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "demo_extract.csv"
            runtime["outputs"]["projection"]["path"] = str(output_path)
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "extract_projection")
            self.assertTrue(output_path.exists())
            self.assertEqual(summary["counts"]["rows_projected"], 800)

    def test_demo_split_profile_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "profiles" / "demo_split.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = Path(tmpdir) / "demo_split_a.csv"
            path_b = Path(tmpdir) / "demo_split_b.csv"
            runtime["outputs"]["split"]["path_a"] = str(path_a)
            runtime["outputs"]["split"]["path_b"] = str(path_b)
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "split_alternating_rows")
            self.assertTrue(path_a.exists())
            self.assertTrue(path_b.exists())
            self.assertEqual(summary["counts"]["rows_a"], 500)
            self.assertEqual(summary["counts"]["rows_b"], 500)

    def test_demo_full_process_profile_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "profiles" / "demo_full_process.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "demo_full_process.csv"
            runtime["outputs"]["records"]["path"] = str(output_path)
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "process_full_records")
            self.assertTrue(output_path.exists())
            self.assertEqual(summary["counts"]["processed_records"], 1000)
            self.assertEqual(summary["stage_stats"]["dedupe_records"]["rows_removed"], 1)

    def test_demo_full_custom_job_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "jobs" / "demo_full_custom_job.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "demo_full_custom_job"
            runtime["outputs"]["records"]["base_dir"] = str(base_dir)
            for stage in runtime["stage_sequence"]:
                config = stage.get("config", {})
                if stage.get("name") == "write_records_bundle":
                    config["base_dir"] = str(base_dir)
                if stage.get("name") == "write_records_output":
                    config["base_dir"] = str(base_dir)
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "custom_job")
            self.assertTrue((base_dir / "matched_records.csv").exists())
            self.assertTrue((base_dir / "processed_records.csv").exists())
            self.assertEqual(summary["stage_stats"]["dedupe_records"]["rows_removed"], 1)
            self.assertIn("score_priority", summary["stage_stats"])
            self.assertEqual(summary["stage_stats"]["write_records_bundle"]["counts"]["matched_records"], 600)

    def test_demo_profiled_custom_job_runs_end_to_end(self) -> None:
        job = load_job(PROJECT_ROOT / "jobs" / "demo_profiled_custom_job.yaml")
        runtime = build_runtime_config(job, root_dir=PROJECT_ROOT)

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "demo_profiled_custom_job"
            runtime["outputs"]["records"]["base_dir"] = str(base_dir)
            for stage in runtime["stage_sequence"]:
                config = stage.get("config", {})
                if stage.get("name") == "write_records_bundle":
                    config["base_dir"] = str(base_dir)
            result = execute_runtime(runtime)

            summary = result["summary"]
            self.assertEqual(summary["workflow"], "custom_job")
            self.assertTrue((base_dir / "matched_records.csv").exists())
            self.assertEqual(summary["stage_stats"]["write_records_bundle"]["counts"]["matched_records"], 600)
            self.assertEqual(summary["stage_stats"]["write_records_bundle"]["counts"]["unmatched_records"], 350)
            self.assertEqual(summary["stage_stats"]["write_records_bundle"]["counts"]["review_records"], 50)


if __name__ == "__main__":
    unittest.main()
