"""Behavioral tests for central quota, persistent edge ledgers and reconciliation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from gateway import (
    CapReducedError,
    CentralGateway,
    EdgeSite,
    GrantWindowError,
    IdempotencyConflict,
    NetworkSimulation,
    QuotaExceeded,
    ReceiptConflict,
)
from gateway.replay import replay_trace
from gateway.demo import run_demo


class OnlineQuotaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.path = Path(self.tmp.name) / "center.json"
        self.gateway = CentralGateway(self.path)
        self.gateway.publish_limit(
            account="plant", period="march", cap=100, starts=0, ends=50, at=0
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_reserve_max_then_release_actual_difference(self):
        accepted = self.gateway.admit_online(
            account="plant", site="cloud", request="r1", maximum=30, use="assist", at=1
        )["request"]
        self.assertEqual(accepted["status"], "accepted")
        self.assertEqual(self.gateway.period_totals("plant", "march")["occupied_against_cap"], 30)

        finished = self.gateway.receive_finish(
            account="plant", site="cloud", request="r1", result="complete", used=22, at=3
        )
        self.assertEqual(finished["settlement"]["difference_released"], 8)
        totals = self.gateway.period_totals("plant", "march")
        self.assertEqual(totals["actual_used"], 22)
        self.assertEqual(totals["occupied_against_cap"], 22)
        self.assertEqual(totals["headroom_signed"], 78)

    def test_duplicate_returns_original_but_changed_contract_conflicts(self):
        payload = dict(account="plant", site="cloud", request="dup", maximum=10, use="vision", at=1)
        first = self.gateway.admit_online(payload)
        second = self.gateway.admit_online(payload)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(self.gateway.period_totals("plant", "march")["online_pending_reserved"], 10)

        with self.assertRaises(IdempotencyConflict):
            self.gateway.admit_online({**payload, "maximum": 11})
        with self.assertRaises(IdempotencyConflict):
            self.gateway.admit_online({**payload, "use": "other"})

    def test_timeout_does_not_release_and_unknown_request_survives_restart(self):
        self.gateway.admit_online(
            account="plant", site="cloud", request="slow", maximum=25, at=5
        )
        timeout = self.gateway.record_timeout(
            account="plant", site="cloud", request="slow", at=6
        )
        self.assertFalse(timeout["quota_released"])

        restarted = CentralGateway(self.path)
        request = restarted.get_request("plant", "cloud", "slow")
        self.assertTrue(request["result_unknown"])
        self.assertEqual(request["timeout_count"], 1)
        self.assertEqual(restarted.period_totals("plant", "march")["online_pending_reserved"], 25)

        restarted.receive_finish(
            account="plant", site="cloud", request="slow", result="never_started", at=27
        )
        totals = restarted.period_totals("plant", "march")
        self.assertEqual(totals["actual_used"], 0)
        self.assertEqual(totals["occupied_against_cap"], 0)
        self.assertEqual(totals["headroom_signed"], 100)

    def test_conflicting_receipt_is_rejected(self):
        self.gateway.admit_online(account="plant", site="cloud", request="r", maximum=10, at=1)
        self.gateway.receive_finish(
            account="plant", site="cloud", request="r", result="complete", used=8, at=2
        )
        with self.assertRaises(ReceiptConflict):
            self.gateway.receive_finish(
                account="plant", site="cloud", request="r", result="complete", used=9, at=3
            )


class EdgeLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.path = Path(self.tmp.name) / "bay.json"
        self.edge = EdgeSite("bay-a", self.path)
        self.edge.receive_grant(
            account="plant", site="bay-a", grant="g", period="march",
            units=80, starts=1, ends=20, at=1,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_local_reservation_retry_restart_and_receipt_release(self):
        self.edge.admit_offline(
            account="plant", request="a", grant="g", seq=1, maximum=35, at=3
        )
        self.edge.admit_offline(
            account="plant", request="b", grant="g", seq=2, maximum=30, at=4
        )
        balance = self.edge.grant_balance("g")
        self.assertEqual(balance["available"], 15)
        self.assertEqual(balance["last_seq"], 2)

        restarted = EdgeSite("bay-a", self.path)
        self.assertEqual(restarted.grant_balance("g")["available"], 15)
        retry = restarted.admit_offline(
            account="plant", site="bay-a", request="b", grant="g", seq=2, maximum=30, at=10
        )
        self.assertTrue(retry["duplicate"])
        self.assertEqual(restarted.grant_balance("g")["available"], 15)

        with self.assertRaises(QuotaExceeded):
            restarted.admit_offline(
                account="plant", request="c", grant="g", seq=3, maximum=16, at=5
            )

        restarted.record_receipt(
            account="plant", request="b", result="complete", used=12, at=22
        )
        balance = EdgeSite("bay-a", self.path).grant_balance("g")
        self.assertEqual(balance["actual_used"], 12)
        self.assertEqual(balance["pending_reserved"], 35)
        self.assertEqual(balance["available"], 33)

    def test_outside_window_is_rejected_without_consuming_seq(self):
        with self.assertRaises(GrantWindowError):
            self.edge.admit_offline(
                account="plant", request="late", grant="g", maximum=1, at=20
            )
        balance = self.edge.grant_balance("g")
        self.assertEqual(balance["last_seq"], 0)
        self.assertEqual(balance["available"], 80)

    def test_close_requires_cutoff_and_matches_local_last_seq(self):
        self.edge.admit_offline(account="plant", request="a", grant="g", maximum=10, at=3)
        with self.assertRaises(Exception):
            self.edge.close_grant(account="plant", grant="g", last_seq=1, at=19)
        close = self.edge.close_grant(account="plant", grant="g", last_seq=1, at=21)
        self.assertFalse(close["duplicate"])
        self.assertTrue(self.edge.grant_balance("g")["closed"])


class ReconciliationAndCapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.center_path = self.root / "center.json"
        self.edge_path = self.root / "bay.json"
        self.center = CentralGateway(self.center_path)
        self.edge = EdgeSite("bay-a", self.edge_path)
        self.center.publish_limit(
            account="plant", period="march", cap=120, starts=0, ends=50, at=0
        )
        grant = dict(
            account="plant", site="bay-a", grant="g", period="march",
            units=80, starts=1, ends=20,
        )
        self.center.issue_grant(grant, at=1)
        self.edge.receive_grant(grant, at=1)
        self.edge.admit_offline(
            account="plant", request="offline-a", grant="g", seq=1, maximum=35, at=3
        )
        self.edge.admit_offline(
            account="plant", request="offline-b", grant="g", seq=2, maximum=30, at=4
        )
        self.center.admit_online(
            account="plant", site="cloud", request="online-a", maximum=25, at=5
        )
        self.center.record_timeout(
            account="plant", site="cloud", request="online-a", at=6
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_out_of_order_records_close_gaps_cap_cut_and_final_return(self):
        totals = self.center.period_totals("plant", "march")
        self.assertEqual(totals["occupied_against_cap"], 105)
        self.assertEqual(totals["headroom_signed"], 15)

        close = self.center.receive_close(
            account="plant", site="bay-a", grant="g", last_seq=2, at=21
        )
        self.assertEqual(close["missing_seq"], [1, 2])
        self.assertFalse(close["ready_to_reclaim"])

        # Receipt arrives before its admission proof.
        receipt = self.center.sync_finish(
            account="plant", site="bay-a", request="offline-b",
            result="complete", used=12, at=22,
        )
        self.assertTrue(receipt["request"]["awaiting_admission"])
        report = self.center.account_report(account="plant", period="march")
        self.assertEqual(len(report["unmatched_receipts"]), 1)

        sync1 = self.center.sync_admit(
            account="plant", site="bay-a", grant="g", request="offline-a",
            seq=1, accepted_at=3, maximum=35, at=23,
        )
        self.assertEqual(sync1["grant"]["missing_seq"], [2])

        self.center.publish_limit(
            account="plant", period="march", cap=75, starts=0, ends=50, at=25
        )
        totals = self.center.period_totals("plant", "march")
        self.assertEqual(totals["over_capacity"], 30)
        self.assertFalse(totals["new_occupancy_allowed"])
        service = self.center.customer_service_view("plant", "march")
        self.assertEqual(service["difference_above_new_cap"], 30)
        self.assertTrue(service["held_site_grants"])
        self.assertTrue(service["pending_or_unknown_receipts"])
        with self.assertRaises(CapReducedError):
            self.center.admit_online(
                account="plant", site="cloud", request="blocked", maximum=1, at=25
            )

        sync2 = self.center.sync_admit(
            account="plant", site="bay-a", grant="g", request="offline-b",
            seq=2, accepted_at=4, maximum=30, at=26,
        )
        self.assertEqual(sync2["request"]["status"], "complete")
        self.assertFalse(sync2["grant"]["ready_to_reclaim"])

        settled = self.center.sync_finish(
            account="plant", site="bay-a", request="offline-a",
            result="never_started", at=27,
        )["grant"]
        self.assertEqual(settled["status"], "settled")
        self.assertEqual(settled["actual_used"], 12)
        self.assertEqual(settled["returned_units"], 68)

        self.center.finish(
            account="plant", site="cloud", request="online-a",
            result="complete", used=22, at=28,
        )
        report = CentralGateway(self.center_path).account_report(
            account="plant", period="march"
        )
        balances = report["balances"]
        self.assertEqual(balances["actual_used"], 34)
        self.assertEqual(balances["occupied_against_cap"], 34)
        self.assertEqual(balances["headroom_signed"], 41)
        self.assertEqual(balances["reclaimed_units"], 68)

    def test_manual_reclaim_queue(self):
        manual = CentralGateway(self.root / "manual.json", auto_reclaim=False)
        manual.publish_limit(account="p", period="x", cap=10, starts=0, ends=10, at=0)
        manual.issue_grant(
            account="p", site="s", grant="g", period="x", units=4,
            starts=0, ends=5, at=0,
        )
        manual.sync_admit(
            account="p", site="s", grant="g", request="r", seq=1,
            accepted_at=1, maximum=4, at=6,
        )
        manual.receive_close(account="p", site="s", grant="g", last_seq=1, at=6)
        manual.sync_finish(account="p", site="s", request="r", result="complete", used=1, at=7)
        view = manual.grant_report("g")
        self.assertEqual(view["status"], "reclaimable")
        self.assertEqual(view["reclaimable_units"], 3)
        manual.reclaim_grant("g", at=8)
        self.assertEqual(manual.period_totals("p", "x")["reclaimed_units"], 3)


class PeriodBoundaryTests(unittest.TestCase):
    def test_acceptance_time_controls_period_and_grant_validity(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        center = CentralGateway(root / "center.json")
        edge = EdgeSite("bay-a", root / "edge.json")
        center.publish_limit(account="plant", period="day1", cap=60, starts=0, ends=10, at=0)
        center.publish_limit(account="plant", period="day2", cap=60, starts=10, ends=20, at=10)
        grant = dict(
            account="plant", site="bay-a", grant="gb", period="day1",
            units=50, starts=1, ends=10,
        )
        center.issue_grant(grant, at=1)
        edge.receive_grant(grant, at=1)
        edge.admit_offline(
            account="plant", request="last", grant="gb", seq=1, maximum=50, at=9
        )
        with self.assertRaises(GrantWindowError):
            edge.admit_offline(account="plant", request="late", grant="gb", maximum=1, at=10)

        center.receive_close(account="plant", site="bay-a", grant="gb", last_seq=1, at=11)
        center.sync_finish(
            account="plant", site="bay-a", request="last",
            result="complete", used=40, at=12,
        )
        center.sync_admit(
            account="plant", site="bay-a", grant="gb", request="last", seq=1,
            accepted_at=9, maximum=50, at=13,
        )
        day1 = center.account_report(account="plant", period="day1")["balances"]
        day2 = center.account_report(account="plant", period="day2")["balances"]
        self.assertEqual(day1["actual_used"], 40)
        self.assertEqual(day1["headroom_signed"], 20)
        self.assertEqual(day2["actual_used"], 0)
        self.assertEqual(day2["headroom_signed"], 60)

        with self.assertRaises(GrantWindowError):
            center.sync_admit(
                account="plant", site="bay-a", grant="gb", request="late", seq=2,
                accepted_at=10, maximum=1, at=14,
            )
        report = center.grant_report("gb")
        self.assertEqual(report["status"], "settled")
        self.assertTrue(report["invalid_records"])
        self.assertEqual(report["returned_units"], 10)


class QuotaFlowAndConcurrencyTests(unittest.TestCase):
    def test_returned_grant_quota_can_flow_to_another_site(self):
        with TemporaryDirectory() as directory:
            center = CentralGateway(Path(directory) / "center.json")
            center.publish_limit(account="plant", period="p", cap=100, starts=0, ends=20, at=0)
            grant = dict(account="plant", period="p", units=80, starts=0, ends=10)
            center.issue_grant({**grant, "site": "a", "grant": "ga"}, at=0)
            center.sync_admit(
                account="plant", site="a", grant="ga", request="r", seq=1,
                accepted_at=1, maximum=80, at=11,
            )
            center.receive_close(account="plant", site="a", grant="ga", last_seq=1, at=11)
            center.sync_finish(
                account="plant", site="a", request="r", result="complete", used=12, at=12
            )
            self.assertEqual(center.grant_report("ga")["returned_units"], 68)
            center.issue_grant({**grant, "site": "b", "grant": "gb", "units": 50}, at=13)
            totals = center.period_totals("plant", "p")
            self.assertEqual(totals["occupied_against_cap"], 62)
            self.assertEqual(totals["headroom_signed"], 38)

    def test_online_usage_remains_in_acceptance_period(self):
        with TemporaryDirectory() as directory:
            center = CentralGateway(Path(directory) / "center.json")
            center.publish_limit(account="plant", period="day1", cap=60, starts=0, ends=10, at=0)
            center.publish_limit(account="plant", period="day2", cap=60, starts=10, ends=20, at=10)
            center.admit_online(account="plant", site="cloud", request="r", maximum=10, at=9)
            center.receive_finish(
                account="plant", site="cloud", request="r", result="complete", used=8, at=11
            )
            self.assertEqual(center.period_totals("plant", "day1")["actual_used"], 8)
            self.assertEqual(center.period_totals("plant", "day2")["actual_used"], 0)

    def test_concurrent_admissions_never_overcommit(self):
        from concurrent.futures import ThreadPoolExecutor

        with TemporaryDirectory() as directory:
            center = CentralGateway(Path(directory) / "center.json")
            center.publish_limit(account="plant", period="p", cap=50, starts=0, ends=100, at=0)

            def admit(i: int):
                try:
                    center.admit_online(
                        account="plant", site=f"s{i % 4}", request=f"r{i}", maximum=1, at=i
                    )
                    return True
                except QuotaExceeded:
                    return False

            with ThreadPoolExecutor(max_workers=16) as pool:
                accepted = sum(pool.map(admit, range(100)))
            self.assertEqual(accepted, 50)
            totals = center.period_totals("plant", "p")
            self.assertEqual(totals["online_pending_reserved"], 50)
            self.assertEqual(totals["shared_new_business_available"], 0)
            with self.assertRaises(QuotaExceeded):
                center.admit_online(
                    account="plant", site="overflow", request="overflow", maximum=1, at=99
                )


class SimulationAndTraceTests(unittest.TestCase):
    def test_high_level_disconnect_restart_duplicate_sync(self):
        with TemporaryDirectory() as directory:
            sim = NetworkSimulation(directory)
            sim.publish_limit(account="plant", period="day", cap=100, starts=0, ends=20, at=0)
            sim.connect("bay")
            sim.issue_grant(
                account="plant", site="bay", grant="g", period="day",
                units=50, starts=0, ends=10, at=0,
            )
            sim.disconnect("bay")
            sim.offline_admit(
                site="bay", account="plant", request="r1", grant="g", maximum=20, at=1
            )
            sim.restart_edge("bay")
            retry = sim.offline_admit(
                site="bay", account="plant", request="r1", grant="g", seq=1, maximum=20, at=2
            )
            self.assertTrue(retry["duplicate"])
            sim.provider.finish_edge(
                site="bay", account="plant", request="r1", result="complete", used=8, at=3
            )
            sim.offline_admit(
                site="bay", account="plant", request="r2", grant="g", seq=2, maximum=30, at=4
            )
            sim.provider.finish_edge(
                site="bay", account="plant", request="r2", result="complete", used=30, at=5
            )
            sim.close_edge_grant(
                site="bay", account="plant", grant="g", last_seq=2, at=11
            )
            result = sim.sync_edge("bay", at=12, duplicate_each=True)
            self.assertEqual(result["pending_sync"]["admissions"], [])
            self.assertEqual(result["pending_sync"]["receipts"], [])
            report = sim.reconciliation_report("plant", "day")
            self.assertEqual(report["central"]["balances"]["actual_used"], 38)
            self.assertEqual(report["central"]["balances"]["reclaimed_units"], 12)
            self.assertEqual(report["edge_balances"][0]["available"], 12)

    def test_built_in_demo_reconciles_expected_totals(self):
        with TemporaryDirectory() as directory:
            result = run_demo(Path(directory))
            balances = result["report"]["central"]["balances"]
            self.assertEqual(balances["actual_used"], 34)
            self.assertEqual(balances["headroom_signed"], 41)
            self.assertEqual(balances["reclaimed_units"], 68)

    def test_provided_traces_replay_and_late_admission_is_rejected(self):
        root = Path(__file__).parent / "data"
        unfinished = replay_trace(root / "unfinished-grant.jsonl")
        self.assertTrue(all(event["ok"] for event in unfinished["events"] if event["type"] != "admit" or event["timestamp"] != 10))
        final = unfinished["inspections"][-1]["report"]["balances"]
        self.assertEqual(final["actual_used"], 34)
        self.assertEqual(final["headroom_signed"], 41)
        self.assertEqual(final["reclaimed_units"], 68)

        boundary = replay_trace(root / "period-boundary.jsonl")
        late = next(e for e in boundary["events"] if e["timestamp"] == 10 and e["type"] == "admit")
        self.assertFalse(late["ok"])
        self.assertEqual(late["error"]["code"], "grant_window_error")


if __name__ == "__main__":
    unittest.main()
