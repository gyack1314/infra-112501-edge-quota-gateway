"""Persistent token-quota gateway for central and disconnected edge sites."""

from .central import CentralGateway
from .clock import LogicalClock
from .edge import EdgeSite
from .errors import (
    CapReducedError,
    CloseError,
    GatewayError,
    GrantWindowError,
    IdempotencyConflict,
    NotFoundError,
    QuotaExceeded,
    ReceiptConflict,
    ReconciliationBlocked,
    SequenceConflict,
    ValidationError,
)
from .models import GrantStatus, RequestStatus
from .simulation import NetworkSimulation, SimulatedProvider
from .http_server import create_server, serve

# Descriptive aliases for callers/tests that use role-oriented names.
Gateway = CentralGateway
QuotaGateway = CentralGateway
EdgeGateway = EdgeSite
SiteGateway = EdgeSite


def create_gateway(role: str = "central", **kwargs):
    """Construct a ``central``/``cloud`` or ``edge``/``site`` gateway."""
    normalized = role.strip().lower()
    if normalized in {"central", "center", "cloud"}:
        return CentralGateway(**kwargs)
    if normalized in {"edge", "site"}:
        return EdgeSite(**kwargs)
    raise ValueError("role 必须是 central 或 edge")

__all__ = [
    "CentralGateway",
    "EdgeSite",
    "Gateway",
    "QuotaGateway",
    "EdgeGateway",
    "SiteGateway",
    "create_gateway",
    "LogicalClock",
    "NetworkSimulation",
    "SimulatedProvider",
    "create_server",
    "serve",
    "GatewayError",
    "ValidationError",
    "NotFoundError",
    "IdempotencyConflict",
    "QuotaExceeded",
    "CapReducedError",
    "GrantWindowError",
    "SequenceConflict",
    "CloseError",
    "ReceiptConflict",
    "ReconciliationBlocked",
    "RequestStatus",
    "GrantStatus",
]
