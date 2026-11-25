"""Optional localhost JSON/HTTP adapter for running central and edge as processes.

The domain API does not require HTTP; tests and simulations normally call the
Python classes directly.  This adapter is a thin RPC boundary so operators can
also launch a persistent central node or one edge-site process.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .central import CentralGateway
from .edge import EdgeSite
from .errors import (
    GatewayError,
    IdempotencyConflict,
    NotFoundError,
    QuotaExceeded,
    ValidationError,
)

CENTRAL_METHODS = {
    "publish_limit",
    "issue_grant",
    "admit_online",
    "record_timeout",
    "receive_finish",
    "sync_finish",
    "sync_admit",
    "receive_close",
    "reconcile_grant",
    "reclaim_grant",
    "account_report",
    "customer_service_view",
    "get_request",
    "list_requests",
}

EDGE_METHODS = {
    "receive_grant",
    "admit_offline",
    "record_receipt",
    "close_grant",
    "grant_balance",
    "get_request",
    "pending_sync",
    "mark_admission_synced",
    "mark_receipt_synced",
    "mark_close_synced",
    "mark_center_settled",
}


def _status_for(error: GatewayError) -> int:
    if isinstance(error, NotFoundError):
        return 404
    if isinstance(error, (IdempotencyConflict, QuotaExceeded)):
        return 409
    if isinstance(error, ValidationError):
        return 400
    return 422


def build_handler(node: Any, role: str, site: str | None) -> type[BaseHTTPRequestHandler]:
    allowed = CENTRAL_METHODS if role == "central" else EDGE_METHODS

    class GatewayHTTPHandler(BaseHTTPRequestHandler):
        server_version = f"TokenGateway{role.capitalize()}/1.0"

        def log_message(self, format: str, *args: Any) -> None:  # quieter CLI server
            return

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib naming
            parsed = urlparse(self.path)
            query = {k: values[-1] for k, values in parse_qs(parsed.query).items()}
            try:
                if parsed.path == "/health":
                    self._send_json(200, {"ok": True, "role": role, "site": site})
                elif parsed.path == "/v1/request":
                    if role == "central":
                        result = node.get_request(query["account"], query["site"], query["request"])
                    else:
                        result = node.get_request(query["account"], query["request"])
                    self._send_json(200, result)
                elif parsed.path == "/v1/report" and role == "central":
                    self._send_json(200, node.account_report(query))
                elif parsed.path == "/v1/balance" and role == "edge":
                    self._send_json(200, node.grant_balance(query["grant"]))
                else:
                    self._send_json(404, {"error": "not_found"})
            except GatewayError as error:
                self._send_json(_status_for(error), {"error": error.to_dict()})
            except KeyError as error:
                self._send_json(400, {"error": {"code": "missing_query_parameter", "message": str(error)}})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/v1/rpc":
                self._send_json(404, {"error": "not_found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                method = payload["method"]
                params = payload.get("params") or {}
                if not isinstance(params, dict):
                    raise ValidationError("params 必须是 JSON 对象")
                if method not in allowed:
                    self._send_json(403, {"error": {"code": "method_not_allowed", "method": method}})
                    return
                result = getattr(node, method)(params)
                self._send_json(200, {"ok": True, "result": result})
            except GatewayError as error:
                self._send_json(_status_for(error), {"ok": False, "error": error.to_dict()})
            except (KeyError, json.JSONDecodeError, ValueError) as error:
                self._send_json(400, {"ok": False, "error": {"code": "bad_request", "message": str(error)}})

    return GatewayHTTPHandler


def create_server(
    *,
    role: str,
    state: str | Path,
    site: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> ThreadingHTTPServer:
    if role == "central":
        node: Any = CentralGateway(state)
    elif role == "edge":
        if not site:
            raise ValueError("edge 服务必须提供 --site")
        node = EdgeSite(site, state)
    else:
        raise ValueError("role 必须是 central 或 edge")
    handler = build_handler(node, role, site)
    return ThreadingHTTPServer((host, port), handler)


def serve(
    *,
    role: str,
    state: str | Path,
    site: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> None:
    server = create_server(role=role, state=state, site=site, host=host, port=port)
    actual_host, actual_port = server.server_address[:2]
    print(f"{role} gateway listening on http://{actual_host}:{actual_port} (state={state})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
