#!/usr/bin/env python3
"""Contracts for capacity eligibility, observer gaps and sampled JFR interpretation."""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import io
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("capacity_report", Path(__file__).with_name("capacity-report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class CapacityReporting(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.load = {"started_at_utc": self.start.isoformat(), "scheduled": 100,
            "outcomes": {"202": 100}, "generator": {"scheduled_duration_s": 10, "rate_per_s": 10},
            "accepted_reconciliation": {"passed": True},
            "successful_scheduled_to_response": {"p99_ms": 100}, "generator_schedule_delay": {"p99_ms": 1}}
        self.lag = [{"observed_at": (self.start + timedelta(seconds=s)).isoformat(), "available": True,
                     "lag_records": 1} for s in range(10)]
        self.resources = [{"observed_at": (self.start + timedelta(seconds=s)).isoformat(),
                           "collector": {"bufferRecords": 0}} for s in (0, 5)]

    def test_complete_workload_passes_only_the_stated_budgets(self):
        self.assertTrue(report.workload_report(self.load, self.lag, self.resources)["budget_passed"])

    def test_rejection_overload_or_missing_accepted_ids_cannot_pass(self):
        self.load["outcomes"] = {"202": 90, "429": 10}
        self.assertFalse(report.workload_report(self.load, self.lag, self.resources)["budget_passed"])
        self.load["outcomes"] = {"202": 100}
        self.load["accepted_reconciliation"]["passed"] = False
        self.assertFalse(report.workload_report(self.load, self.lag, self.resources)["budget_passed"])

    def test_missing_observers_or_excess_generator_delay_are_inconclusive(self):
        self.assertFalse(report.workload_report(self.load, [], self.resources)["budget_passed"])
        self.load["generator_schedule_delay"]["p99_ms"] = 51
        self.assertFalse(report.workload_report(self.load, self.lag, self.resources)["budget_passed"])

    def test_samples_outside_the_measurement_window_are_excluded(self):
        outside = {"observed_at": (self.start - timedelta(seconds=1)).isoformat(),
                   "available": True, "lag_records": 1000}
        result = report.workload_report(self.load, [outside, *self.lag], self.resources)
        self.assertEqual(1, result["source_lag_records"]["max"])

    def test_jfr_weights_durations_and_stack_samples_are_not_exact_counters(self):
        at = self.start.isoformat()
        events = [
            {"type": "jdk.ObjectAllocationSample", "values": {"startTime": at, "weight": 2048, "objectClass": {"name": "java/lang/String"}}},
            {"type": "jdk.GarbageCollection", "values": {"startTime": at, "sumOfPauses": "PT0.002S"}},
            {"type": "jdk.ExecutionSample", "values": {"startTime": at, "stackTrace": {"frames": [{"method": {"type": {"name": "dev/sharad/Test"}, "name": "work"}}]}}},
            {"type": "jdk.FileWrite", "values": {"startTime": at, "duration": "PT0.025S"}},
            {"type": "jdk.CPULoad", "values": {"startTime": at, "jvmUser": 0.1, "jvmSystem": 0.1}},
            {"type": "jdk.CPULoad", "values": {"startTime": (self.start + timedelta(seconds=10)).isoformat(), "jvmUser": 0.1, "jvmSystem": 0.1}}]
        result = report.jfr_events(events, self.start, self.start + timedelta(seconds=10))
        self.assertEqual(2048, result["estimated_allocation_bytes"])
        self.assertEqual(2, result["gc_pause_total_ms"])
        self.assertEqual(25, result["thresholded_file_io_ms"]["max"])
        self.assertEqual([("dev.sharad.Test.work", 1)], result["sampled_top_methods"])

    def test_invalid_sample_weight_or_duration_is_rejected(self):
        with self.assertRaises(ValueError):
            report.seconds("unknown")
        with self.assertRaises(ValueError):
            report.jfr_events([{"type": "jdk.ObjectAllocationSample", "values": {
                "startTime": self.start.isoformat(), "weight": -1}}], self.start, self.start)

    def test_jfr_streaming_parser_rejects_truncation_and_oversized_events(self):
        events = [{"type": "jdk.CPULoad", "values": {}}, {"type": "jdk.ExecutionSample", "values": {}}]
        self.assertEqual(events, list(report.iter_events(io.StringIO(json.dumps({"recording": {"events": events}})))))
        with self.assertRaises(ValueError):
            list(report.iter_events(io.StringIO('{"recording":{"events":[{"type":')))
        with self.assertRaises(ValueError):
            list(report.iter_events(io.StringIO('{}')))
        with self.assertRaisesRegex(ValueError, "parser budget"):
            list(report.iter_events(io.StringIO(json.dumps({"recording": {"events": [{"value": "x" * 200_000}]}})), 100_000))

    def test_partial_matrix_cannot_be_presented_as_complete_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "all three rates"):
                report.aggregate(Path(directory))

    def test_passing_rate_requires_every_repetition_and_a_consistent_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for rate in (10, 50, 150):
                for repeat in (1, 2, 3):
                    case = root / f"{rate}-{repeat}"
                    case.mkdir()
                    (case / "environment.json").write_text(json.dumps({"mode": "full", "dirty": False,
                        "cpu": '{"lscpu":[]}', "runner": '{}', "revision": "current", "tree": "tree",
                        "java": "Java 21", "started_at": self.start.isoformat()}))
                    for phase in ("spool-baseline", "acknowledged", "acknowledged-profiled"):
                        directory = case / phase
                        directory.mkdir()
                        count = rate * 600
                        load = {**self.load, "generator": {"scheduled_duration_s": 600, "rate_per_s": rate},
                            "scheduled": count, "outcomes": {"202": count},
                            "accepted_reconciliation": {"accepted": count, "visible": count, "unresolved_ids": [], "passed": True},
                            "working_tree_dirty": False, "revision": "current", "acknowledged_per_s_including_inflight_drain": rate,
                            "successful_http_elapsed": {"p99_ms": 10}, "ambiguous_client_outcomes": {"count": 0}}
                        value = {"rate": rate, "repeat": repeat, "phase": phase, "capacity_eligible": True,
                            "source_revision": report.BASELINE_REVISION if phase == "spool-baseline" else "current", "load": load, "budget_passed": rate != 150,
                            "budget": {"workload": rate != 150}, "source_lag_records": {}, "buffer_records": {}, "resources": {}, "workload": {}}
                        if phase == "acknowledged-profiled":
                            value["jfr"] = {"collector": {}, "worker": {}}
                            for role in ("collector", "worker"):
                                (directory / (role + "-jfr.json")).write_text('{}')
                        (directory / "report.json").write_text(json.dumps(value))
            self.assertEqual(50, report.aggregate(root)["highest_passing_tested_offered_rate"])
            path = root / "50-2/acknowledged/report.json"
            value = json.loads(path.read_text())
            value["budget_passed"] = False
            value["budget"]["workload"] = False
            path.write_text(json.dumps(value))
            self.assertEqual(10, report.aggregate(root)["highest_passing_tested_offered_rate"])
            value["load"]["revision"] = "different"
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "mixes generator revisions"):
                report.aggregate(root)


if __name__ == "__main__":
    unittest.main()
