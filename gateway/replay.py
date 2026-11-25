"""Replay the documented JSONL event contracts through central and edge nodes."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Any

from .central import CentralGateway
from .edge import EdgeSite
from .errors import GatewayError

try:  # Reuse the repository's envelope validator when running from a checkout.
    from trace_io import read_trace
except ImportError:  # pragma: no cover - allows importing the package from elsewhere
    def read_trace(path: Path, allowed_types: set[str]):
        with Path(path).open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                event = json.loads(line)
                if event.get("type") not in allowed_types:
                    raise ValueError(f"{path}:{line_number} unknown event type")
                yield event


ALLOWED_TYPES = {
    "admit",
    "close",
    "finish",
    "grant",
    "inspect",
    "limit",
    "sync_admit",
    "sync_finish",
    "timeout",
}


class TraceReplayer:
    def __init__(self, state_dir: str | Path | None = None, *, auto_reclaim: bool = True):
        if state_dir is None:
            state_dir = Path(tempfile.mkdtemp(prefix="gateway-replay-"))
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.edge_dir = self.state_dir / "edges"
        self.edge_dir.mkdir(exist_ok=True)
        self.center = CentralGateway(self.state_dir / "center.json", auto_reclaim=auto_reclaim)
        self.edges: dict[str, EdgeSite] = {}
        self.events: list[dict[str, Any]] = []
        self.inspections: list[dict[str, Any]] = []

    def edge(self, site: str) -> EdgeSite:
        if site not in self.edges:
            self.edges[site] = EdgeSite(site, self.edge_dir / f"edge-{site}.json")
        return self.edges[site]

    def handle_event(self, index: int, event: dict[str, Any]) -> dict[str, Any]:
        event_type = event["type"]
        data = dict(event["data"])
        timestamp = event["timestamp"]
        data.setdefault("timestamp", timestamp)
        outcome: dict[str, Any] = {"index": index, "timestamp": timestamp, "type": event_type}

        try:
            result = self._dispatch(event_type, data)
            outcome["ok"] = True
            outcome["result"] = result
            if event_type == "inspect":
                inspection = {"index": index, "timestamp": timestamp, "query": dict(event["data"]), "report": result}
                self.inspections.append(inspection)
                outcome["inspection_index"] = len(self.inspections) - 1
        except GatewayError as error:
            outcome["ok"] = False
            outcome["error"] = error.to_dict()
        return outcome

    def _dispatch(self, event_type: str, data: dict[str, Any]) -> Any:
        if event_type == "limit":
            return self.center.publish_limit(data)
        if event_type == "grant":
            result = self.center.issue_grant(data)
            self.edge(data["site"]).receive_grant(data)
            return result
        if event_type == "admit":
            if data.get("grant") is None:
                return self.center.admit_online(data)
            # A grant without seq is an attempted acceptance; the edge policy
            # rejects it if the window/sequence/balance rules are not met.
            return self.edge(data["site"]).admit_offline(data)
        if event_type == "timeout":
            return self.center.record_timeout(data)
        if event_type == "finish":
            return self.center.receive_finish(data, via="provider")
        if event_type == "sync_finish":
            edge = self.edge(data["site"])
            local = edge.record_receipt(data)
            central = self.center.sync_finish(data)
            edge.mark_receipt_synced(data["account"], data["request"], at=data["timestamp"])
            return {"edge": local, "central": central}
        if event_type == "sync_admit":
            result = self.center.sync_admit(data)
            edge = self.edge(data["site"])
            edge.mark_admission_synced(data["account"], data["request"], at=data["timestamp"])
            return result
        if event_type == "close":
            edge = self.edge(data["site"])
            local = edge.close_grant(data)
            central = self.center.receive_close(data)
            edge.mark_close_synced(data["grant"], at=data["timestamp"])
            return {"edge": local, "central": central}
        if event_type == "inspect":
            if data.get("grant"):
                return self.center.grant_report(data["grant"])
            if data.get("site") and not data.get("period"):
                edge = self.edge(data["site"])
                if data.get("grant"):
                    return edge.grant_balance(data["grant"])
                return [edge.grant_balance(g["grant_id"]) for g in edge.state["grants"]]
            return self.center.account_report(data)
        raise ValueError(f"未处理的事件类型 {event_type}")

    def replay(self, path: str | Path) -> dict[str, Any]:
        path = Path(path)
        for index, event in enumerate(read_trace(path, ALLOWED_TYPES), 1):
            outcome = self.handle_event(index, event)
            self.events.append(outcome)
        return {
            "trace": str(path),
            "state_dir": str(self.state_dir),
            "event_count": len(self.events),
            "events": self.events,
            "inspections": self.inspections,
        }


def replay_trace(path: str | Path, state_dir: str | Path | None = None) -> dict[str, Any]:
    return TraceReplayer(state_dir).replay(path)
