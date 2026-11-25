"""A small deterministic network simulation for central/edge rehearsals."""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

from .central import CentralGateway
from .edge import EdgeSite
from .errors import GatewayError


class DisconnectedError(GatewayError):
    code = "network_disconnected"


class SimulatedProvider:
    """Issues synthetic final receipts through the simulation network."""

    def __init__(self, simulation: "NetworkSimulation"):
        self.simulation = simulation

    def finish_online(
        self,
        *,
        account: str,
        site: str,
        request: str,
        result: str = "complete",
        used: int | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        if site not in self.simulation.connected:
            raise DisconnectedError(f"站点 {site} 当前与中心断开，在线回执无法直达中心")
        return self.simulation.center.receive_finish(
            {"account": account, "site": site, "request": request, "result": result, "used": used, "at": at},
            via="provider",
        )

    def finish_edge(
        self,
        *,
        account: str,
        site: str,
        request: str,
        result: str = "complete",
        used: int | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """Record a provider receipt at the edge.

        If the site is connected it is forwarded immediately; otherwise it
        remains in the durable outbox for reconnect reconciliation.
        """
        edge = self.simulation.edge(site)
        local = edge.record_receipt(
            {"account": account, "site": site, "request": request, "result": result, "used": used, "at": at}
        )
        forwarded = None
        if site in self.simulation.connected:
            envelope = edge.receipt_envelope(account, request)
            forwarded = self.simulation.center.sync_finish({**envelope, "at": at})
            edge.mark_receipt_synced(account, request, at=at)
        return {"local": local, "forwarded": forwarded}


class NetworkSimulation:
    """Owns persistent central and edge state files and simulated connectivity."""

    def __init__(self, state_dir: str | Path | None = None, *, auto_reclaim: bool = True):
        if state_dir is None:
            state_dir = Path(tempfile.mkdtemp(prefix="quotagateway-"))
        self.root = Path(state_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "edges").mkdir(exist_ok=True)
        self.center_path = self.root / "center.json"
        self.center = CentralGateway(self.center_path, auto_reclaim=auto_reclaim)
        self._edges: dict[str, EdgeSite] = {}
        self.connected: set[str] = set()
        self.provider = SimulatedProvider(self)

    # ------------------------------------------------------------- lifecycle

    def edge_path(self, site: str) -> Path:
        return self.root / "edges" / f"edge-{site}.json"

    def edge(self, site: str) -> EdgeSite:
        if site not in self._edges:
            self._edges[site] = EdgeSite(site, self.edge_path(site))
        return self._edges[site]

    def restart_edge(self, site: str) -> EdgeSite:
        """Simulate a local process restart by dropping the in-memory object."""
        self._edges.pop(site, None)
        return self.edge(site)

    def restart_center(self) -> CentralGateway:
        self.center = CentralGateway(self.center_path)
        return self.center

    def disconnect(self, site: str) -> None:
        self.connected.discard(site)

    def connect(self, site: str) -> None:
        self.connected.add(site)

    # ------------------------------------------------------------- operations

    def publish_limit(self, **data: Any) -> dict[str, Any]:
        return self.center.publish_limit(data)

    def issue_grant(self, *, deliver_to_edge: bool = True, require_connected: bool = True, **data: Any) -> dict[str, Any]:
        site = data["site"]
        if require_connected and site not in self.connected:
            raise DisconnectedError("在线授出份额时站点应处于连接状态")
        result = self.center.issue_grant(data)
        if deliver_to_edge:
            self.edge(site).receive_grant(data)
        return result

    def online_admit(self, **data: Any) -> dict[str, Any]:
        site = data["site"]
        if site not in self.connected:
            raise DisconnectedError("站点断开时不能走在线准入；请使用 offline_admit 和本地 grant")
        return self.center.admit_online(data)

    def timeout_online(self, **data: Any) -> dict[str, Any]:
        # A client timeout can occur regardless of current simulated link state.
        return self.center.record_timeout(data)

    def offline_admit(self, site: str, **data: Any) -> dict[str, Any]:
        data["site"] = site
        return self.edge(site).admit_offline(data)

    def restarted_offline_admit(self, site: str, **data: Any) -> dict[str, Any]:
        self.restart_edge(site)
        return self.offline_admit(site, **data)

    def close_edge_grant(self, site: str, **data: Any) -> dict[str, Any]:
        data["site"] = site
        return self.edge(site).close_grant(data)

    # ------------------------------------------------------------- reconnect

    def sync_edge(
        self,
        site: str,
        *,
        at: int,
        receipts_before_admissions: bool = True,
        duplicate_each: bool = False,
        close_first: bool = True,
    ) -> dict[str, Any]:
        """Reconnect and submit outbox items.

        By default the final receipts are sent before admission proofs, exactly
        the out-of-order case that central reconciliation must tolerate.
        """
        self.connect(site)
        edge = self.edge(site)
        outbox = edge.pending_sync()
        sent: dict[str, list[dict[str, Any]]] = {"closes": [], "receipts": [], "admissions": []}

        def twice_if_requested(call: Any, envelope: dict[str, Any], kind: str) -> None:
            first = call({**envelope, "at": at})
            sent[kind].append(first)
            if duplicate_each:
                second = call({**envelope, "at": at})
                sent[kind].append(second)

        if close_first:
            for envelope in outbox["closes"]:
                twice_if_requested(self.center.receive_close, envelope, "closes")
                edge.mark_close_synced(envelope["grant"], at=at)

        receipt_items = outbox["receipts"]
        admission_items = outbox["admissions"]
        if receipts_before_admissions:
            for envelope in receipt_items:
                twice_if_requested(self.center.sync_finish, envelope, "receipts")
                edge.mark_receipt_synced(envelope["account"], envelope["request"], at=at)
            for envelope in admission_items:
                twice_if_requested(self.center.sync_admit, envelope, "admissions")
                edge.mark_admission_synced(envelope["account"], envelope["request"], at=at)
        else:
            for envelope in admission_items:
                twice_if_requested(self.center.sync_admit, envelope, "admissions")
                edge.mark_admission_synced(envelope["account"], envelope["request"], at=at)
            for envelope in receipt_items:
                twice_if_requested(self.center.sync_finish, envelope, "receipts")
                edge.mark_receipt_synced(envelope["account"], envelope["request"], at=at)

        # Close may have been sent before later outbox items existed; reconcile all.
        grants = [g["grant_id"] for g in edge.state["grants"]]
        grant_reports = [self.center.reconcile_grant(gid, at=at) for gid in grants]
        for report in grant_reports:
            if report["status"] == "settled" and report.get("settlement"):
                edge.mark_center_settled(
                    report["grant"],
                    returned_units=report["settlement"]["returned_units"],
                    actual_used=report["settlement"]["actual_used"],
                    at=at,
                )
        return {"sent": sent, "grants": grant_reports, "pending_sync": edge.pending_sync()}

    # ------------------------------------------------------------- reporting

    def site_reports(self, *, at: int | None = None) -> list[dict[str, Any]]:
        reports = []
        edge_dir = self.root / "edges"
        for path in sorted(edge_dir.glob("edge-*.json")):
            site = path.name[len("edge-") : -len(".json")]
            edge = EdgeSite(site, path)
            for grant in edge.state["grants"]:
                reports.append(edge.grant_balance(grant["grant_id"], at=at))
        return reports

    def reconciliation_report(self, account: str, period: str, *, at: int | None = None) -> dict[str, Any]:
        report_params = {"account": account, "period": period}
        if at is not None:
            report_params["at"] = at
        central = self.center.account_report(report_params)
        return {
            "central": central,
            "edge_balances": [
                report
                for report in self.site_reports(at=at)
                if report["account"] == account and report["period"] == period
            ],
        }
