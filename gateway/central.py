"""Central shared-quota gateway and reconciliation ledger."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .clock import LogicalClock
from .concurrency import synchronized
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
from .store import JsonStore
from .validation import ensure_window, fingerprint, optional_use, require_int, require_str


def _initial_state() -> dict[str, Any]:
    return {"version": 1, "limits": [], "grants": [], "requests": [], "audit": []}


@synchronized
class CentralGateway:
    """Owns the subscription cap and cross-site reconciliation.

    All mutating methods persist before returning.  Token accounting is derived
    from durable limit, grant, admission and receipt records, which makes a
    restart equivalent to reopening the same state file.
    """

    def __init__(
        self,
        path: str | Path | JsonStore | None = None,
        *,
        clock: Callable[[], int] | None = None,
        auto_reclaim: bool = True,
    ):
        if isinstance(path, JsonStore):
            self.store = path
        else:
            if path is not None:
                candidate = Path(path)
                if candidate.exists() and candidate.is_dir():
                    path = candidate / "center.json"
            self.store = JsonStore(path, initial=_initial_state())
        self.state = self.store.data
        for record in self.state.get("requests", []):
            if record.get("status") == "completed":
                record["status"] = RequestStatus.COMPLETE
        self.clock = clock or LogicalClock(0)
        self.auto_reclaim = auto_reclaim

    # ------------------------------------------------------------------ basics

    def _at(self, at: int | None) -> int:
        if at is None:
            value = int(self.clock())
        else:
            if type(at) is not int or isinstance(at, bool) or at < 0:
                raise ValidationError("逻辑时间必须是非负整数", details={"at": at})
            value = at
        if isinstance(self.clock, LogicalClock):
            self.clock.set(value)
        return value

    def _save(self) -> None:
        self.store.save()

    def _audit(self, at: int, action: str, **details: Any) -> None:
        self.state.setdefault("audit", []).append({"at": at, "action": action, "details": details})

    @staticmethod
    def _data(payload: dict[str, Any] | None, kwargs: dict[str, Any]) -> dict[str, Any]:
        data = dict(payload or {})
        data.update(kwargs)
        return data

    def reload(self) -> "CentralGateway":
        self.state = self.store.reload()
        return self

    # ------------------------------------------------------------------ lookups

    def _find_limit(self, account: str, period: str) -> dict[str, Any] | None:
        return next(
            (x for x in self.state["limits"] if x["account"] == account and x["period"] == period),
            None,
        )

    def _require_limit(self, account: str, period: str) -> dict[str, Any]:
        limit = self._find_limit(account, period)
        if limit is None:
            raise NotFoundError(
                f"未找到额度期间 {account}/{period}",
                details={"account": account, "period": period},
            )
        return limit

    def limit_for_time(self, account: str, at: int) -> dict[str, Any] | None:
        matches = [
            x
            for x in self.state["limits"]
            if x["account"] == account and x["starts"] <= at < x["ends"]
        ]
        if len(matches) > 1:
            raise ValidationError(
                f"账户 {account} 在时间 {at} 命中多个期间",
                details={"at": at, "periods": [x["period"] for x in matches]},
            )
        return matches[0] if matches else None

    def _find_grant(self, grant_id: str) -> dict[str, Any] | None:
        return next((x for x in self.state["grants"] if x["grant_id"] == grant_id), None)

    def _require_grant(self, grant_id: str) -> dict[str, Any]:
        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError(f"未找到站点份额 {grant_id}", details={"grant": grant_id})
        return grant

    def _find_request_record(
        self, account: str, site: str, request: str
    ) -> dict[str, Any] | None:
        return next(
            (
                x
                for x in self.state["requests"]
                if x["account"] == account and x["site"] == site and x["request"] == request
            ),
            None,
        )

    def _request_view(self, record: dict[str, Any]) -> dict[str, Any]:
        view = deepcopy(record)
        maximum = record.get("maximum")
        used = record.get("used")
        terminal = record.get("status") in RequestStatus.TERMINAL
        view["result_known"] = terminal
        view["result_unknown"] = not terminal and record.get("status") != RequestStatus.REJECTED
        if isinstance(maximum, int):
            view["reserved_units"] = 0 if terminal else maximum
            if terminal and isinstance(used, int):
                view["released_difference"] = maximum - used
            else:
                view["released_difference"] = 0
        else:
            view["reserved_units"] = 0
            view["released_difference"] = 0
        return view

    def get_request(self, account: str, site: str, request: str) -> dict[str, Any]:
        record = self._find_request_record(account, site, request)
        if record is None:
            raise NotFoundError(
                "未找到请求记录",
                details={"account": account, "site": site, "request": request},
            )
        return self._request_view(record)

    query_request = get_request
    query_request_status = get_request

    def list_requests(
        self,
        *,
        account: str | None = None,
        site: str | None = None,
        status: str | None = None,
        include_invalid: bool = False,
    ) -> list[dict[str, Any]]:
        result = []
        for record in self.state["requests"]:
            if account is not None and record["account"] != account:
                continue
            if site is not None and record["site"] != site:
                continue
            if status is not None and record["status"] != status:
                continue
            if not include_invalid and not record.get("valid", True):
                continue
            result.append(self._request_view(record))
        return result

    # ------------------------------------------------------------- quota limits

    def publish_limit(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        period = require_str(data, "period")
        cap = require_int(data, "cap", minimum=0)
        starts = require_int(data, "starts", minimum=0)
        ends = require_int(data, "ends", minimum=1)
        ensure_window(starts, ends)

        limit = self._find_limit(account, period)
        if limit is None:
            limit = {
                "account": account,
                "period": period,
                "cap": cap,
                "starts": starts,
                "ends": ends,
                "created_at": at,
                "updated_at": at,
                "cap_effective_at": at,
                "history": [{"at": at, "cap": cap}],
            }
            self.state["limits"].append(limit)
        else:
            if limit["starts"] != starts or limit["ends"] != ends:
                raise ValidationError(
                    "同一 period 标识的时间窗口不能改变",
                    details={"existing": [limit["starts"], limit["ends"]], "incoming": [starts, ends]},
                )
            if limit["cap"] != cap:
                limit["history"].append({"at": at, "cap": cap})
                limit["cap_effective_at"] = at
            limit["cap"] = cap
            limit["updated_at"] = at

        totals = self.period_totals(account, period)
        warning = None
        if totals["over_capacity"] > 0:
            warning = {
                "code": "cap_below_existing_occupancy",
                "difference": totals["over_capacity"],
                "message": "新上限低于既有占用；新增占用必须停止，等待客服核对",
            }
        self._audit(at, "limit", account=account, period=period, cap=cap, warning=warning)
        self._save()
        return {"limit": deepcopy(limit), "totals": totals, "warning": warning}

    limit = publish_limit

    # ------------------------------------------------------------- calculations

    def period_totals(self, account: str, period: str) -> dict[str, Any]:
        limit = self._require_limit(account, period)
        requests = [
            r
            for r in self.state["requests"]
            if r.get("account") == account
            and r.get("period") == period
            and r.get("valid", True)
        ]
        online = [r for r in requests if not r.get("grant_id")]
        offline = [r for r in requests if r.get("grant_id")]

        online_pending = sum(
            r.get("maximum") or 0 for r in online if r["status"] == RequestStatus.ACCEPTED
        )
        online_actual = sum(
            r.get("used") or 0 for r in online if r["status"] in RequestStatus.TERMINAL
        )
        offline_actual = sum(
            r.get("used") or 0 for r in offline if r["status"] in RequestStatus.TERMINAL
        )
        actual_used = online_actual + offline_actual
        unmatched_receipt_actual = sum(
            r.get("used") or 0
            for r in self.state["requests"]
            if r.get("account") == account
            and r.get("awaiting_admission")
            and r.get("status") in RequestStatus.TERMINAL
        )
        provisional_unmatched_in_window = sum(
            r.get("used") or 0
            for r in self.state["requests"]
            if r.get("account") == account
            and r.get("awaiting_admission")
            and r.get("status") in RequestStatus.TERMINAL
            and isinstance(r.get("receipt_received_at"), int)
            and limit["starts"] <= r["receipt_received_at"] < limit["ends"]
        )

        grants = [
            g
            for g in self.state["grants"]
            if g["account"] == account and g["period"] == period
        ]
        open_principal = 0
        settled_actual = 0
        reclaimable_units = 0
        blocked_contractual_remainder = 0
        reclaimed_units = 0
        for grant in grants:
            grant_requests = [
                r
                for r in self.state["requests"]
                if r.get("grant_id") == grant["grant_id"] and r.get("valid", True)
            ]
            grant_actual = sum(
                r.get("used") or 0
                for r in grant_requests
                if r["status"] in RequestStatus.TERMINAL
            )
            if grant["status"] == GrantStatus.SETTLED:
                settled_actual += grant_actual
                reclaimed_units += grant.get("returned_units", 0)
            else:
                open_principal += grant["units"]
                remainder = max(0, grant["units"] - grant_actual)
                if grant["status"] == GrantStatus.RECLAIMABLE:
                    reclaimable_units += grant.get("reclaimable_units", remainder)
                else:
                    # Upper-bound arithmetic remainder from attached receipts;
                    # blockers explain why it cannot yet be returned.
                    blocked_contractual_remainder += remainder

        occupied = online_pending + online_actual + open_principal + settled_actual
        cap = limit["cap"]
        headroom = cap - occupied
        return {
            "account": account,
            "period": period,
            "cap": cap,
            "occupied_against_cap": occupied,
            "headroom_signed": headroom,
            "shared_new_business_available": max(0, headroom),
            "over_capacity": max(0, -headroom),
            "cap_reduction_blocking": headroom < 0,
            "new_occupancy_allowed": headroom >= 0,
            "actual_used": actual_used,
            "unmatched_receipt_actual_account_wide": unmatched_receipt_actual,
            "provisional_unmatched_actual_in_window": provisional_unmatched_in_window,
            "online_pending_reserved": online_pending,
            "online_actual_used": online_actual,
            "offline_known_actual_used": offline_actual,
            "open_grant_principal": open_principal,
            "settled_grant_actual": settled_actual,
            "reclaimable_units": reclaimable_units,
            "blocked_contractual_remainder": blocked_contractual_remainder,
            "reclaimed_units": reclaimed_units,
        }

    def _assert_capacity(self, account: str, period: str, additional: int) -> dict[str, Any]:
        totals = self.period_totals(account, period)
        if not totals["new_occupancy_allowed"]:
            view = self.customer_service_view(account, period)
            raise CapReducedError(
                "账户当前占用已超过新上限，所有新增占用均被阻止",
                details={"totals": totals, "service": view},
            )
        if totals["occupied_against_cap"] + additional > totals["cap"]:
            raise QuotaExceeded(
                "共享额度不足，无法留足本次上限",
                details={"requested_maximum": additional, "totals": totals},
            )
        return totals

    # ------------------------------------------------------------------ grants

    def issue_grant(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        grant_id = require_str(data, "grant")
        period = require_str(data, "period")
        units = require_int(data, "units", minimum=0)
        starts = require_int(data, "starts", minimum=0)
        ends = require_int(data, "ends", minimum=1)
        ensure_window(starts, ends)

        existing = self._find_grant(grant_id)
        if existing is not None:
            same = all(
                existing.get(k) == v
                for k, v in {
                    "account": account,
                    "site": site,
                    "period": period,
                    "units": units,
                    "starts": starts,
                    "ends": ends,
                }.items()
            )
            if not same:
                raise IdempotencyConflict(
                    f" grant 标识 {grant_id} 已用于不同授权契约",
                    details={
                        "grant": grant_id,
                        "existing": {k: existing.get(k) for k in ("account", "site", "period", "units", "starts", "ends")},
                        "incoming": data,
                    },
                )
            return {"grant": deepcopy(existing), "duplicate": True}

        limit = self._require_limit(account, period)
        if starts < limit["starts"] or ends > limit["ends"]:
            raise ValidationError(
                "站点份额窗口必须位于额度期间内，不能跨期间",
                details={
                    "grant_window": [starts, ends],
                    "period_window": [limit["starts"], limit["ends"]],
                },
            )

        self._assert_capacity(account, period, units)
        grant = {
            "grant_id": grant_id,
            "account": account,
            "site": site,
            "period": period,
            "units": units,
            "starts": starts,
            "ends": ends,
            "issued_at": at,
            "status": GrantStatus.OPEN,
            "last_seq": 0,
            "close": None,
            "settled": False,
            "settlement": None,
            "reclaimable_units": 0,
            "returned_units": 0,
        }
        self.state["grants"].append(grant)
        self._audit(at, "grant", grant=grant_id, account=account, site=site, units=units)
        self._save()
        return {"grant": deepcopy(grant), "duplicate": False}

    grant = issue_grant

    # --------------------------------------------------------- online admission

    def admit_online(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        request = require_str(data, "request")
        maximum = require_int(data, "maximum", minimum=0)
        use = optional_use(data)

        if data.get("grant") is not None or data.get("seq") is not None:
            raise ValidationError("在线准入不能携带 grant/seq；离线受理请使用 sync_admit")

        existing = self._find_request_record(account, site, request)
        if existing is not None and existing.get("maximum") is not None:
            incoming_fp = fingerprint(maximum, use)
            if fingerprint(existing.get("maximum"), existing.get("use")) != incoming_fp:
                raise IdempotencyConflict(
                    "同一 account/site/request 被用于不同用途或上限",
                    details={
                        "key": {"account": account, "site": site, "request": request},
                        "existing": {
                            "maximum": existing.get("maximum"),
                            "use": existing.get("use"),
                            "grant": existing.get("grant_id"),
                            "seq": existing.get("seq"),
                        },
                        "incoming": incoming_fp,
                    },
                )
            return {"request": self._request_view(existing), "duplicate": True}

        period = self.limit_for_time(account, at)
        if period is None:
            raise NotFoundError(
                f"账户 {account} 在逻辑时间 {at} 没有生效的额度期间",
                details={"account": account, "at": at},
            )

        # A receipt may have arrived before the admission delivery.  Attach it
        # instead of reserving and then forgetting to settle.
        pre_existing_terminal = (
            existing is not None and existing["status"] in RequestStatus.TERMINAL
        )
        if pre_existing_terminal:
            used = existing.get("used") or 0
            if used > maximum:
                raise ReceiptConflict(
                    "先到回执的 used 大于受理上限",
                    details={"used": used, "maximum": maximum},
                )

        self._assert_capacity(account, period["period"], maximum)

        if existing is None:
            record = {
                "account": account,
                "site": site,
                "request": request,
                "maximum": maximum,
                "use": use,
                "status": RequestStatus.ACCEPTED,
                "period": period["period"],
                "grant_id": None,
                "seq": None,
                "accepted_at": at,
                "admission_received_at": at,
                "timeout_count": 0,
                "timeouts": [],
                "finished_at": None,
                "used": None,
                "receipt_via": None,
                "valid": True,
            }
            self.state["requests"].append(record)
        else:
            # timeout placeholder or receipt-only record
            existing.update(
                {
                    "maximum": maximum,
                    "use": use,
                    "period": period["period"],
                    "accepted_at": at,
                    "admission_received_at": at,
                    "valid": True,
                }
            )
            if existing["status"] not in RequestStatus.TERMINAL:
                existing["status"] = RequestStatus.ACCEPTED
            record = existing

        self._audit(at, "admit_online", account=account, site=site, request=request, maximum=maximum)
        self._save()

        settlement = None
        if record["status"] in RequestStatus.TERMINAL:
            settlement = self._settlement_view(record)
        return {"request": self._request_view(record), "duplicate": False, "settlement": settlement}

    admit = admit_online

    # ----------------------------------------------------------------- timeout

    def record_timeout(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        request = require_str(data, "request")

        record = self._find_request_record(account, site, request)
        created_placeholder = False
        if record is None:
            # The client timeout alone proves neither acceptance nor failure.
            # Persist that unknown key so it remains queryable after restart.
            record = {
                "account": account,
                "site": site,
                "request": request,
                "maximum": None,
                "use": None,
                "status": RequestStatus.UNKNOWN,
                "period": None,
                "grant_id": None,
                "seq": None,
                "accepted_at": None,
                "admission_received_at": None,
                "timeout_count": 0,
                "timeouts": [],
                "finished_at": None,
                "used": None,
                "receipt_via": None,
                "valid": True,
            }
            self.state["requests"].append(record)
            created_placeholder = True

        record["timeout_count"] = int(record.get("timeout_count", 0)) + 1
        record.setdefault("timeouts", []).append({"at": at})
        record["last_timeout_at"] = at
        # Deliberately no quota release and no terminal status change.
        self._audit(at, "timeout", account=account, site=site, request=request)
        self._save()
        return {
            "request": self._request_view(record),
            "placeholder_created": created_placeholder,
            "quota_released": False,
        }

    timeout = record_timeout

    # ----------------------------------------------------------------- receipts

    @staticmethod
    def _normalize_receipt_result(result: Any, used: Any) -> tuple[str, int]:
        if result in ("complete", "completed", RequestStatus.COMPLETE):
            if used is None:
                raise ValidationError("complete 回执必须包含整数 used")
            if type(used) is not int or isinstance(used, bool) or used < 0:
                raise ValidationError("used 必须是非负整数", details={"used": used})
            return RequestStatus.COMPLETED, used
        if result == RequestStatus.NEVER_STARTED:
            if used is None:
                used = 0
            if type(used) is not int or isinstance(used, bool) or used != 0:
                raise ValidationError("never_started 回执的 used 必须为 0 或省略")
            return RequestStatus.NEVER_STARTED, 0
        raise ValidationError(
            "result 必须是 complete 或 never_started", details={"result": result}
        )

    def receive_finish(
        self,
        payload: dict[str, Any] | None = None,
        /,
        *,
        via: str = "provider",
        **kwargs: Any,
    ) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        request = require_str(data, "request")
        result, used = self._normalize_receipt_result(data.get("result"), data.get("used"))

        record = self._find_request_record(account, site, request)
        duplicate = False
        if record is None:
            # Receipt may arrive before the offline admission proof.  Preserve
            # it and match it when sync_admit arrives.
            record = {
                "account": account,
                "site": site,
                "request": request,
                "maximum": None,
                "use": None,
                "status": result,
                "period": None,
                "grant_id": None,
                "seq": None,
                "accepted_at": None,
                "admission_received_at": None,
                "timeout_count": 0,
                "timeouts": [],
                "finished_at": at,
                "used": used,
                "receipt_via": via,
                "receipt_received_at": at,
                "valid": True,
                "awaiting_admission": True,
            }
            self.state["requests"].append(record)
        elif record["status"] in RequestStatus.TERMINAL:
            if record["status"] != result or (record.get("used") or 0) != used:
                raise ReceiptConflict(
                    "同一请求收到互相冲突的最终回执",
                    details={
                        "key": {"account": account, "site": site, "request": request},
                        "existing": {"result": record["status"], "used": record.get("used")},
                        "incoming": {"result": result, "used": used},
                    },
                )
            duplicate = True
        else:
            maximum = record.get("maximum")
            if isinstance(maximum, int) and used > maximum:
                raise ReceiptConflict(
                    "回执 used 超过受理上限",
                    details={"used": used, "maximum": maximum},
                )
            record["status"] = result
            record["used"] = used
            record["finished_at"] = at
            record["receipt_via"] = via
            record["receipt_received_at"] = at
            if record.get("maximum") is None:
                record["awaiting_admission"] = True
            else:
                record["awaiting_admission"] = False

        self._audit(
            at,
            "finish",
            account=account,
            site=site,
            request=request,
            result=result,
            used=used,
            via=via,
            duplicate=duplicate,
        )
        self._save()

        settlement = self._settlement_view(record)
        grant_report = None
        if record.get("grant_id"):
            grant = self._find_grant(record["grant_id"])
            if grant is not None:
                grant_report = self.reconcile_grant(grant["grant_id"], at=at)
        return {
            "request": self._request_view(record),
            "duplicate": duplicate,
            "settlement": settlement,
            "grant": grant_report,
        }

    finish = receive_finish

    def sync_finish(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("via", "site_sync")
        return self.receive_finish(payload, **kwargs)

    def _settlement_view(self, record: dict[str, Any]) -> dict[str, Any]:
        maximum = record.get("maximum")
        used = record.get("used")
        if record["status"] not in RequestStatus.TERMINAL or not isinstance(used, int):
            return {"final": False}
        if not isinstance(maximum, int):
            return {"final": True, "used": used, "difference_released_after_admission_match": None}
        return {
            "final": True,
            "used": used,
            "maximum": maximum,
            "difference_released": maximum - used,
        }

    # ---------------------------------------------------------- offline records

    def sync_admit(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        request = require_str(data, "request")
        grant_id = require_str(data, "grant")
        seq = require_int(data, "seq", minimum=1)
        accepted_at = require_int(data, "accepted_at", minimum=0)
        maximum = require_int(data, "maximum", minimum=0)
        use = optional_use(data)

        grant = self._require_grant(grant_id)
        if grant["account"] != account or grant["site"] != site:
            raise IdempotencyConflict(
                "grant 不属于给定 account/site",
                details={"grant": grant_id, "grant_owner": {k: grant[k] for k in ("account", "site")}, "incoming": data},
            )

        existing = self._find_request_record(account, site, request)
        if existing is not None and existing.get("maximum") is not None and existing.get("valid", True):
            comparable = {
                "maximum": maximum,
                "use": use,
                "grant_id": grant_id,
                "seq": seq,
                "accepted_at": accepted_at,
            }
            existing_comparable = {
                "maximum": existing.get("maximum"),
                "use": existing.get("use"),
                "grant_id": existing.get("grant_id"),
                "seq": existing.get("seq"),
                "accepted_at": existing.get("accepted_at"),
            }
            if comparable != existing_comparable:
                raise IdempotencyConflict(
                    "同一 account/site/request 的离线受理证明与原记录不一致",
                    details={"existing": existing_comparable, "incoming": comparable},
                )
            report = self.reconcile_grant(grant_id, at=at)
            return {"request": self._request_view(existing), "duplicate": True, "grant": report}

        if existing is not None and not existing.get("valid", True):
            raise IdempotencyConflict(
                "该请求编号已有被隔离的无效受理记录，不能改写为有效记录",
                details={"existing": self._request_view(existing)},
            )

        # Sequence number must not identify another request.
        seq_owner = next(
            (
                r
                for r in self.state["requests"]
                if r.get("grant_id") == grant_id
                and r.get("seq") == seq
                and r.get("valid", True)
                and r["request"] != request
            ),
            None,
        )
        if seq_owner is not None:
            raise SequenceConflict(
                f"grant {grant_id} 的 seq {seq} 已属于另一个请求",
                details={"seq": seq, "existing_request": seq_owner["request"], "incoming_request": request},
            )

        # Validity is judged at the edge acceptance instant, never at reconnect.
        # This cannot overwrite an already-validated idempotent record because
        # exact duplicates and contract conflicts were handled above.
        if not (grant["starts"] <= accepted_at < grant["ends"]):
            invalid = self._persist_invalid_admission(
                at=at,
                account=account,
                site=site,
                request=request,
                grant_id=grant_id,
                seq=seq,
                accepted_at=accepted_at,
                maximum=maximum,
                use=use,
                code="accepted_at_outside_grant_window",
                message="受理时点超出 grant 有效窗口，补传也不得进入结算",
            )
            self._save()
            raise GrantWindowError(
                "离线受理时间不在份额有效窗口内，已隔离且不进入结算",
                details={"record": invalid, "grant_window": [grant["starts"], grant["ends"]]},
            )

        if existing is None:
            record = {
                "account": account,
                "site": site,
                "request": request,
                "maximum": maximum,
                "use": use,
                "status": RequestStatus.ACCEPTED,
                "period": grant["period"],
                "grant_id": grant_id,
                "seq": seq,
                "accepted_at": accepted_at,
                "admission_received_at": at,
                "timeout_count": 0,
                "timeouts": [],
                "finished_at": None,
                "used": None,
                "receipt_via": None,
                "valid": True,
                "awaiting_admission": False,
            }
            self.state["requests"].append(record)
        else:
            # Match an out-of-order terminal receipt.
            if (existing.get("used") or 0) > maximum:
                raise ReceiptConflict(
                    "先到回执的 used 大于补来受理证明的 maximum",
                    details={"used": existing.get("used"), "maximum": maximum},
                )
            existing.update(
                {
                    "maximum": maximum,
                    "use": use,
                    "period": grant["period"],
                    "grant_id": grant_id,
                    "seq": seq,
                    "accepted_at": accepted_at,
                    "admission_received_at": at,
                    "valid": True,
                }
            )
            if existing["status"] in RequestStatus.TERMINAL:
                existing["awaiting_admission"] = False
            record = existing

        grant["last_seq"] = max(int(grant.get("last_seq") or 0), seq)
        self._audit(at, "sync_admit", grant=grant_id, seq=seq, request=request, accepted_at=accepted_at)
        self._save()
        report = self.reconcile_grant(grant_id, at=at)
        return {"request": self._request_view(record), "duplicate": False, "grant": report}

    def _persist_invalid_admission(self, **details: Any) -> dict[str, Any]:
        account = details["account"]
        site = details["site"]
        request = details["request"]
        existing = self._find_request_record(account, site, request)
        record = existing or {
            "account": account,
            "site": site,
            "request": request,
            "timeout_count": 0,
            "timeouts": [],
        }
        record.update(
            {
                "maximum": details["maximum"],
                "use": details["use"],
                "status": RequestStatus.REJECTED,
                "period": self._find_grant(details["grant_id"])["period"],
                "grant_id": details["grant_id"],
                "seq": details["seq"],
                "accepted_at": details["accepted_at"],
                "admission_received_at": details["at"],
                "valid": False,
                "rejection_code": details["code"],
                "rejection_message": details["message"],
                "finished_at": None,
                "used": record.get("used"),
            }
        )
        if existing is None:
            self.state["requests"].append(record)
        self._audit(details["at"], "invalid_sync_admit", request=request, code=details["code"])
        return deepcopy(record)

    # ------------------------------------------------------------------- close

    def receive_close(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        grant_id = require_str(data, "grant")
        last_seq = require_int(data, "last_seq", minimum=0)

        grant = self._require_grant(grant_id)
        if grant["account"] != account or grant["site"] != site:
            raise IdempotencyConflict("close 中的 account/site 与 grant 不匹配", details={"grant": grant_id})
        if at < grant["ends"]:
            raise CloseError(
                "份额窗口尚未截止，不能提交最终收口声明",
                details={"at": at, "ends": grant["ends"]},
            )

        close = grant.get("close")
        if close is not None:
            if close["last_seq"] != last_seq:
                # Keep the original declaration visible; a larger frontier is a
                # reconciliation conflict rather than silently rewriting proof.
                raise SequenceConflict(
                    "同一 grant 的收口 last_seq 被改成不同值",
                    details={"existing_last_seq": close["last_seq"], "incoming_last_seq": last_seq},
                )
            duplicate = True
        else:
            grant["close"] = {"last_seq": last_seq, "declared_at": at, "received_at": at}
            if grant["status"] == GrantStatus.OPEN:
                grant["status"] = GrantStatus.CLOSE_DECLARED
            duplicate = False

        self._audit(at, "close", grant=grant_id, last_seq=last_seq, duplicate=duplicate)
        self._save()
        report = self.reconcile_grant(grant_id, at=at)
        report["duplicate_close"] = duplicate
        return report

    close = receive_close
    close_grant = receive_close
    submit_online = admit_online
    online_admit = admit_online
    report_finish = receive_finish
    provider_finish = receive_finish

    # --------------------------------------------------------- grant reconcile

    def grant_report(self, grant_id: str, *, at: int | None = None) -> dict[str, Any]:
        grant = self._require_grant(grant_id)
        return self._grant_view(grant, at=self._at(at) if at is not None else None)

    def _grant_view(self, grant: dict[str, Any], *, at: int | None = None) -> dict[str, Any]:
        gid = grant["grant_id"]
        records = [
            r
            for r in self.state["requests"]
            if r.get("grant_id") == gid and r.get("valid", True)
        ]
        invalid = [
            self._request_view(r)
            for r in self.state["requests"]
            if r.get("grant_id") == gid and not r.get("valid", True)
        ]
        by_seq = {r["seq"]: r for r in records if isinstance(r.get("seq"), int)}
        actual = sum(r.get("used") or 0 for r in records if r["status"] in RequestStatus.TERMINAL)
        pending = sum(r.get("maximum") or 0 for r in records if r["status"] == RequestStatus.ACCEPTED)
        unfinished = [
            self._request_view(r)
            for r in records
            if r["status"] == RequestStatus.ACCEPTED or r["status"] == RequestStatus.UNKNOWN
        ]

        blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        missing_seq: list[int] = []
        above_close: list[int] = []
        close = grant.get("close")
        last_seq = close["last_seq"] if close else None
        if close is None:
            blockers.append({"code": "close_not_declared", "message": "站点尚未提交收口声明"})
        else:
            missing_seq = [seq for seq in range(1, last_seq + 1) if seq not in by_seq]
            if missing_seq:
                blockers.append(
                    {
                        "code": "missing_admission_records",
                        "message": "收口声明中的部分 seq 缺少对应受理证明",
                        "missing_seq": missing_seq,
                    }
                )
            above_close = sorted(seq for seq in by_seq if last_seq is not None and seq > last_seq)
            if above_close:
                blockers.append(
                    {
                        "code": "seq_above_close_frontier",
                        "message": "出现了收口 last_seq 之外的受理记录",
                        "seq": above_close,
                    }
                )
        if unfinished:
            blockers.append(
                {
                    "code": "receipts_pending",
                    "message": "仍有已受理请求没有最终回执",
                    "requests": [
                        {"account": r["account"], "site": r["site"], "request": r["request"]}
                        for r in unfinished
                    ],
                }
            )
        if invalid:
            warnings.append(
                {
                    "code": "invalid_proofs_quarantined",
                    "message": "存在被窗口规则隔离、不参与结算的补传受理证明",
                    "requests": [
                        {"request": r["request"], "seq": r.get("seq"), "code": r.get("rejection_code")}
                        for r in invalid
                    ],
                }
            )
        if actual > grant["units"]:
            blockers.append(
                {
                    "code": "actual_usage_exceeds_grant",
                    "message": "最终用量超过授权份额，需客服追差",
                    "actual_used": actual,
                    "units": grant["units"],
                }
            )

        ready = not blockers
        remainder = max(0, grant["units"] - actual) if actual <= grant["units"] else 0
        center_known_available = grant["units"] - actual - pending
        closed_or_settled = close is not None or grant["status"] == GrantStatus.SETTLED
        time_valid = True if at is None else grant["starts"] <= at < grant["ends"]
        spendable_now = 0 if closed_or_settled or not time_valid else max(0, center_known_available)
        return {
            "grant": gid,
            "account": grant["account"],
            "site": grant["site"],
            "period": grant["period"],
            "window": [grant["starts"], grant["ends"]],
            "units": grant["units"],
            "status": grant["status"],
            "close": deepcopy(close),
            "known_seq": sorted(by_seq),
            "missing_seq": missing_seq,
            "seq_above_close": above_close,
            "request_count": len(records),
            "actual_used": actual,
            "pending_reserved": pending,
            "center_known_available": center_known_available,
            "spendable_now": spendable_now,
            "center_known_occupied": actual + pending,
            "outstanding_principal": 0 if grant["status"] == GrantStatus.SETTLED else grant["units"],
            "contractual_remainder": remainder,
            "reclaimable_units": grant.get("reclaimable_units", 0),
            "returned_units": grant.get("returned_units", 0),
            "settlement": deepcopy(grant.get("settlement")),
            "ready_to_reclaim": ready,
            "blockers": blockers,
            "warnings": warnings,
            "requests": [self._request_view(r) for r in sorted(records, key=lambda x: x.get("seq") or 0)],
            "invalid_records": invalid,
        }

    def reconcile_grant(self, grant_id: str, *, at: int | None = None) -> dict[str, Any]:
        at = self._at(at)
        grant = self._require_grant(grant_id)
        view = self._grant_view(grant, at=at)

        if view["ready_to_reclaim"] and grant["status"] != GrantStatus.SETTLED:
            actual = view["actual_used"]
            remainder = view["contractual_remainder"]
            grant["reclaimable_units"] = remainder
            if self.auto_reclaim:
                grant["status"] = GrantStatus.SETTLED
                grant["settled"] = True
                grant["returned_units"] = remainder
                grant["reclaimable_units"] = 0
                grant["settlement"] = {
                    "settled_at": at,
                    "actual_used": actual,
                    "returned_units": remainder,
                    "request_count": view["request_count"],
                }
                self._audit(at, "grant_settled", grant=grant_id, actual=actual, returned=remainder)
                self._save()
            else:
                grant["status"] = GrantStatus.RECLAIMABLE
                self._save()
            view = self._grant_view(grant, at=at)
        return view

    def reclaim_grant(self, grant_id: str, *, at: int | None = None) -> dict[str, Any]:
        at = self._at(at)
        grant = self._require_grant(grant_id)
        view = self.reconcile_grant(grant_id, at=at)
        if grant["status"] == GrantStatus.SETTLED:
            return view
        if grant["status"] != GrantStatus.RECLAIMABLE:
            raise ReconciliationBlocked("份额尚不能收回余量", details=view)
        remainder = grant.get("reclaimable_units", 0)
        grant["status"] = GrantStatus.SETTLED
        grant["settled"] = True
        grant["returned_units"] = remainder
        grant["reclaimable_units"] = 0
        grant["settlement"] = {
            "settled_at": at,
            "actual_used": view["actual_used"],
            "returned_units": remainder,
            "request_count": view["request_count"],
        }
        self._audit(at, "grant_reclaimed", grant=grant_id, returned=remainder)
        self._save()
        return self._grant_view(grant, at=at)

    def reconcile_all(self, *, at: int | None = None) -> list[dict[str, Any]]:
        at = self._at(at)
        return [self.reconcile_grant(g["grant_id"], at=at) for g in self.state["grants"]]

    # ----------------------------------------------------------------- reports

    def account_report(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        account = require_str(data, "account")
        period = require_str(data, "period")
        raw_at = data.get("at", data.get("timestamp"))
        report_at = self._at(raw_at) if raw_at is not None else None
        limit = self._require_limit(account, period)
        totals = self.period_totals(account, period)
        grant_views = [
            self._grant_view(g, at=report_at)
            for g in self.state["grants"]
            if g["account"] == account and g["period"] == period
        ]

        all_requests = [
            self._request_view(r)
            for r in self.state["requests"]
            if r["account"] == account and r.get("period") == period and r.get("valid", True)
        ]
        unknown_requests = [r for r in all_requests if r["result_unknown"]]
        timed_out = [r for r in all_requests if r.get("timeout_count", 0) > 0 and not r["result_known"]]
        unmatched_receipts = [
            self._request_view(r)
            for r in self.state["requests"]
            if r["account"] == account
            and r.get("awaiting_admission")
            and r.get("status") in RequestStatus.TERMINAL
        ]
        missing_records = []
        for view in grant_views:
            for blocker in view["blockers"]:
                if blocker["code"] == "missing_admission_records":
                    missing_records.append(
                        {"grant": view["grant"], "site": view["site"], "missing_seq": blocker["missing_seq"]}
                    )

        site_balances = [
            {
                "site": view["site"],
                "grant": view["grant"],
                "status": view["status"],
                "authorized_units": view["units"],
                "center_known_available": view["center_known_available"],
                "center_known_unspent": view["center_known_available"],
                "spendable_now_center_view": view["spendable_now"],
                "outstanding_principal": view["outstanding_principal"],
                "note": "断网期间的可接单余额以边缘持久化 spendable_balance 为准",
            }
            for view in grant_views
        ]
        return {
            "account": account,
            "period": period,
            "window": [limit["starts"], limit["ends"]],
            "cap": limit["cap"],
            "cap_effective_at": limit.get("cap_effective_at"),
            "cap_history": deepcopy(limit.get("history", [])),
            "balances": totals,
            "recovery": {
                "reclaimable_now": totals["reclaimable_units"],
                "blocked_contractual_remainder": totals["blocked_contractual_remainder"],
                "held_grant_principal": totals["open_grant_principal"],
                "already_returned": totals["reclaimed_units"],
                "explanation": "只有 close 缺口补齐且每笔受理均有最终回执后，blocked remainder 才可收回",
            },
            "usable": {
                "shared_online_or_new_grant": totals["shared_new_business_available"],
                "new_occupancy_allowed": totals["new_occupancy_allowed"],
                "sites": site_balances,
            },
            "grants": grant_views,
            "unknown_requests": unknown_requests,
            "timed_out_requests": timed_out,
            "unmatched_receipts": unmatched_receipts,
            "missing_records": missing_records,
            "customer_service": {
                "difference_above_new_cap": totals["over_capacity"],
                "occupied_against_cap": totals["occupied_against_cap"],
                "current_cap": totals["cap"],
                "reclaimable_now": totals["reclaimable_units"],
                "blocked_contractual_remainder": totals["blocked_contractual_remainder"],
                "held_grant_principal": totals["open_grant_principal"],
                "already_returned": totals["reclaimed_units"],
                "pending_or_unknown_receipts": [
                    {
                        "account": r["account"],
                        "site": r["site"],
                        "request": r["request"],
                        "maximum": r.get("maximum"),
                        "status": r["status"],
                        "timeout_count": r.get("timeout_count", 0),
                    }
                    for r in unknown_requests
                ],
                "unmatched_final_receipts": [
                    {
                        "account": r["account"],
                        "site": r["site"],
                        "request": r["request"],
                        "result": r["status"],
                        "used": r.get("used"),
                    }
                    for r in unmatched_receipts
                ],
                "held_site_grants": [
                    {
                        "grant": g["grant"],
                        "site": g["site"],
                        "status": g["status"],
                        "outstanding_principal": g["outstanding_principal"],
                        "contractual_remainder": g["contractual_remainder"],
                        "missing_seq": g["missing_seq"],
                        "blockers": g["blockers"],
                    }
                    for g in grant_views
                    if g["status"] != GrantStatus.SETTLED
                ],
                "missing_records": missing_records,
            },
        }

    inspect = account_report
    reconcile_account = account_report
    account_balance = account_report

    def customer_service_view(self, account: str, period: str | None = None) -> dict[str, Any]:
        if period is not None:
            return self.account_report(account=account, period=period)["customer_service"]
        periods = [x["period"] for x in self.state["limits"] if x["account"] == account]
        return {p: self.account_report(account=account, period=p)["customer_service"] for p in periods}
