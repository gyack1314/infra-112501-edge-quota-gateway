"""Status constants shared by central and edge gateways."""

from __future__ import annotations


class RequestStatus:
    #: A timeout or provider event is known, but no admission contract exists yet.
    UNKNOWN = "unknown"
    #: Durably admitted; quota is reserved. No terminal provider receipt yet.
    ACCEPTED = "accepted"
    #: Provider reported completion with a final used-token count.
    COMPLETE = "complete"
    #: Backwards-friendly alias for callers that prefer the adjective form.
    COMPLETED = COMPLETE
    #: Provider proved that inference never started; final usage is zero.
    NEVER_STARTED = "never_started"
    #: A late/out-of-window proof was preserved but excluded from settlement.
    REJECTED = "rejected"

    TERMINAL = {COMPLETE, "completed", NEVER_STARTED}


class GrantStatus:
    OPEN = "open"
    CLOSE_DECLARED = "close_declared"
    #: All records/receipts are present; remainder may be reclaimed.
    RECLAIMABLE = "reclaimable"
    #: Remainder has been returned to the shared account quota.
    SETTLED = "settled"
    DISPUTED = "disputed"
