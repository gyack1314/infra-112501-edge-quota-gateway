"""Errors raised by the token-quota gateway."""

from __future__ import annotations

from typing import Any


class GatewayError(Exception):
    """Base class for expected gateway policy and reconciliation errors."""

    code = "gateway_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class ValidationError(GatewayError):
    code = "validation_error"


class NotFoundError(GatewayError):
    code = "not_found"


class IdempotencyConflict(GatewayError):
    """A natural key was reused with a different request contract."""

    code = "idempotency_conflict"


class QuotaExceeded(GatewayError):
    code = "quota_exceeded"


class CapReducedError(QuotaExceeded):
    """New business is blocked because existing occupancy is above a new cap."""

    code = "cap_reduced_blocked"


class GrantWindowError(GatewayError):
    code = "grant_window_error"


class SequenceConflict(GatewayError):
    code = "sequence_conflict"


class CloseError(GatewayError):
    code = "close_error"


class ReceiptConflict(GatewayError):
    code = "receipt_conflict"


class ReconciliationBlocked(GatewayError):
    code = "reconciliation_blocked"
