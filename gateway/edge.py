"""Persistent edge-site gateway and local grant ledger."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .clock import LogicalClock
from .concurrency import synchronized
from .errors import (
    CloseError,
    GrantWindowError,
    IdempotencyConflict,
    NotFoundError,
    QuotaExceeded,
    ReceiptConflict,
    SequenceConflict,
    ValidationError,
)
from .models import RequestStatus
from .store import JsonStore
from .validation import optional_use, require_int, require_str


def _initial_state(site: str | None = None) -> dict[str, Any]:
    return {"version": 1, "site": site, "grants": [], "requests": [], "audit": []}


@synchronized
class EdgeSite:
    """Maintains one site's accepted work and occupied grant balance.

    The edge never admits from the account-wide cap or from the last balance
    reported to central.  It spends only a durably received grant and reloads
    its own JSON state after a process restart.
    """

    def __init__(
        self,
        site: str,
        path: str | Path | JsonStore | None = None,
        *,
        clock: Callable[[], int] | None = None,
    ):
        if not isinstance(site, str) or not site:
            raise ValidationError("site 必须是非空字符串")
        if isinstance(path, JsonStore):
            self.store = path
        else:
            if path is not None:
                candidate = Path(path)
                if candidate.exists() and candidate.is_dir():
                    path = candidate / f"edge-{site}.json"
            self.store = JsonStore(path, initial=_initial_state(site))
        self.state = self.store.data
        for record in self.state.get("requests", []):
            if record.get("status") == "completed":
                record["status"] = RequestStatus.COMPLETE
        if self.state.get("site") is None:
            self.state["site"] = site
        elif self.state.get("site") != site:
            raise ValidationError(
                "边缘状态文件属于另一个 site",
                details={"expected": site, "actual": self.state.get("site")},
            )
        self.site = site
        self.clock = clock or LogicalClock(0)

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

    def reload(self) -> "EdgeSite":
        self.state = self.store.reload()
        return self

    # ------------------------------------------------------------------ lookups

    def _find_grant(self, grant_id: str) -> dict[str, Any] | None:
        return next((g for g in self.state["grants"] if g["grant_id"] == grant_id), None)

    def grant_balance(self, grant_id: str, *, at: int | None = None) -> dict[str, Any]:
        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError(f"本地点没有份额 {grant_id}", details={"site": self.site, "grant": grant_id})
        records = [r for r in self.state["requests"] if r.get("grant_id") == grant_id]
        pending = sum(r["maximum"] for r in records if r["status"] == RequestStatus.ACCEPTED)
        actual = sum(r.get("used") or 0 for r in records if r["status"] in RequestStatus.TERMINAL)
        occupied = pending + actual
        unspent = grant["units"] - occupied
        returned = int(grant.get("returned_units", 0) or 0)
        closed = grant.get("close") is not None
        time_valid = True if at is None else grant["starts"] <= at < grant["ends"]
        spendable = 0 if closed or not time_valid else unspent
        return {
            "site": self.site,
            "account": grant["account"],
            "grant": grant_id,
            "period": grant["period"],
            "window": [grant["starts"], grant["ends"]],
            "units": grant["units"],
            "last_seq": grant.get("last_seq", 0),
            "accepted_count": len(records),
            "pending_reserved": pending,
            "actual_used": actual,
            "occupied": occupied,
            # "available" remains the arithmetic unused authorization for
            # compatibility; spendable_balance is the value an edge may admit on.
            "available": unspent,
            "unspent_balance": unspent,
            "spendable_balance": spendable,
            "pending_return_to_center": max(0, unspent - returned) if closed else 0,
            "returned_to_center": returned,
            "closed": closed,
            "close": deepcopy(grant.get("close")),
            "center_settlement": deepcopy(grant.get("center_settlement")),
        }

    balance = grant_balance

    def get_request(self, account: str, request: str) -> dict[str, Any]:
        record = next(
            (
                r
                for r in self.state["requests"]
                if r["account"] == account and r["site"] == self.site and r["request"] == request
            ),
            None,
        )
        if record is None:
            raise NotFoundError("边缘未找到请求", details={"site": self.site, "account": account, "request": request})
        view = deepcopy(record)
        terminal = record["status"] in RequestStatus.TERMINAL
        view["result_known"] = terminal
        view["available_released"] = (
            record["maximum"] - record["used"] if terminal and isinstance(record.get("used"), int) else 0
        )
        return view

    query_request = get_request
    query_request_status = get_request

    # ------------------------------------------------------------------ grants

    def receive_grant(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = require_str(data, "site")
        if site != self.site:
            raise ValidationError("不能把其他站点的份额写入本边缘状态", details={"local_site": self.site, "grant_site": site})
        grant_id = require_str(data, "grant")
        period = require_str(data, "period")
        units = require_int(data, "units", minimum=0)
        starts = require_int(data, "starts", minimum=0)
        ends = require_int(data, "ends", minimum=1)
        if starts >= ends:
            raise ValidationError("grant 窗口必须 starts < ends")

        existing = self._find_grant(grant_id)
        if existing is not None:
            same = all(existing.get(k) == v for k, v in {
                "account": account, "site": site, "period": period, "units": units,
                "starts": starts, "ends": ends,
            }.items())
            if not same:
                raise IdempotencyConflict("边缘已存在不同的同标识 grant", details={"grant": grant_id})
            return {"grant": deepcopy(existing), "duplicate": True}

        grant = {
            "grant_id": grant_id,
            "account": account,
            "site": site,
            "period": period,
            "units": units,
            "starts": starts,
            "ends": ends,
            "received_at": at,
            "last_seq": 0,
            "close": None,
        }
        self.state["grants"].append(grant)
        self._audit(at, "receive_grant", grant=grant_id, units=units)
        self._save()
        return {"grant": deepcopy(grant), "duplicate": False}

    # ------------------------------------------------------------- admission

    def admit_offline(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = data.get("site", self.site)
        if site != self.site:
            raise ValidationError("admit 的 site 与边缘实例不一致", details={"local_site": self.site, "site": site})
        request = require_str(data, "request")
        maximum = require_int(data, "maximum", minimum=0)
        use = optional_use(data)
        grant_id = require_str(data, "grant")
        requested_seq = require_int(data, "seq", minimum=1, required=False)

        existing = next(
            (
                r
                for r in self.state["requests"]
                if r["account"] == account and r["site"] == self.site and r["request"] == request
            ),
            None,
        )
        # Retries must trace the original acceptance and must never reserve twice.
        if existing is not None:
            same = (
                existing.get("maximum") == maximum
                and existing.get("use") == use
                and existing.get("grant_id") == grant_id
                and (requested_seq is None or existing.get("seq") == requested_seq)
            )
            if not same:
                raise IdempotencyConflict(
                    "同一请求编号在边缘被用于不同用途、上限、grant 或 seq",
                    details={
                        "key": {"account": account, "site": self.site, "request": request},
                        "existing": {
                            "maximum": existing.get("maximum"),
                            "use": existing.get("use"),
                            "grant": existing.get("grant_id"),
                            "seq": existing.get("seq"),
                        },
                        "incoming": {"maximum": maximum, "use": use, "grant": grant_id, "seq": requested_seq},
                    },
                )
            return {"request": self.get_request(account, request), "duplicate": True}

        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError(
                "边缘没有该份额，不能凭账户缓存总额接单",
                details={"site": self.site, "grant": grant_id},
            )
        if grant["account"] != account:
            raise ValidationError("grant 不属于该账户", details={"grant": grant_id, "account": account})
        if grant.get("close") is not None:
            raise CloseError("份额已经本地收口，不能继续受理", details={"grant": grant_id})

        # This is the decisive offline check.  Reconnect arrival time is irrelevant.
        if not (grant["starts"] <= at < grant["ends"]):
            raise GrantWindowError(
                "本地受理时点不在份额有效窗口内",
                details={"accepted_at": at, "window": [grant["starts"], grant["ends"]]},
            )

        next_seq = int(grant.get("last_seq", 0)) + 1
        seq = requested_seq if requested_seq is not None else next_seq
        if seq != next_seq:
            raise SequenceConflict(
                "新的本地受理必须使用连续 seq；重试原请求应复用原 seq",
                details={"expected_seq": next_seq, "incoming_seq": seq},
            )

        balance = self.grant_balance(grant_id)
        if maximum > balance["available"]:
            raise QuotaExceeded(
                "站点份额余额不足，不能接单；边缘不使用账户缓存总额",
                details={"maximum": maximum, "balance": balance},
            )

        record = {
            "account": account,
            "site": self.site,
            "request": request,
            "maximum": maximum,
            "use": use,
            "grant_id": grant_id,
            "seq": seq,
            "accepted_at": at,
            "status": RequestStatus.ACCEPTED,
            "used": None,
            "finished_at": None,
            "admission_synced": False,
            "receipt_synced": False,
            "sync_attempts": 0,
        }
        self.state["requests"].append(record)
        grant["last_seq"] = seq
        self._audit(at, "admit_offline", grant=grant_id, seq=seq, request=request, maximum=maximum)
        self._save()
        return {"request": deepcopy(record), "duplicate": False, "balance": self.grant_balance(grant_id)}

    admit = admit_offline
    reserve = admit_offline

    # -------------------------------------------------------------- receipts

    def record_receipt(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = data.get("site", self.site)
        if site != self.site:
            raise ValidationError("回执 site 与边缘实例不一致")
        request = require_str(data, "request")
        result = data.get("result")
        used = data.get("used")

        record = next(
            (
                r
                for r in self.state["requests"]
                if r["account"] == account and r["site"] == self.site and r["request"] == request
            ),
            None,
        )
        if record is None:
            raise NotFoundError("边缘无法为未受理请求记录回执", details={"request": request})

        if result in ("complete", "completed", RequestStatus.COMPLETE):
            result = RequestStatus.COMPLETE
            if type(used) is not int or isinstance(used, bool) or used < 0:
                raise ValidationError("complete 回执必须包含非负整数 used")
        elif result == RequestStatus.NEVER_STARTED:
            used = 0 if used is None else used
            if used != 0:
                raise ValidationError("never_started 的 used 必须为 0")
        else:
            raise ValidationError("result 必须是 complete 或 never_started")

        if used > record["maximum"]:
            raise ReceiptConflict("回执 used 超过本地受理上限", details={"used": used, "maximum": record["maximum"]})

        if record["status"] in RequestStatus.TERMINAL:
            if record["status"] != result or (record.get("used") or 0) != used:
                raise ReceiptConflict("边缘收到冲突回执", details={"request": request})
            duplicate = True
        else:
            record["status"] = result
            record["used"] = used
            record["finished_at"] = at
            record["receipt_synced"] = False
            duplicate = False

        self._audit(at, "receipt", request=request, result=result, used=used, duplicate=duplicate)
        self._save()
        return {"request": self.get_request(account, request), "duplicate": duplicate,
                "balance": self.grant_balance(record["grant_id"])}

    finish = record_receipt

    # ------------------------------------------------------------------ close

    def close_grant(self, payload: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        data = self._data(payload, kwargs)
        at = self._at(data.get("at") if "at" in data else data.get("timestamp"))
        account = require_str(data, "account")
        site = data.get("site", self.site)
        if site != self.site:
            raise ValidationError("close site 与边缘实例不一致")
        grant_id = require_str(data, "grant")
        incoming_last_seq = require_int(data, "last_seq", minimum=0, required=False)

        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError("边缘没有该 grant", details={"grant": grant_id})
        if grant["account"] != account:
            raise ValidationError("grant 不属于该账户")
        if at < grant["ends"]:
            raise CloseError("窗口截止前不能形成最终收口声明", details={"at": at, "ends": grant["ends"]})

        local_last_seq = int(grant.get("last_seq", 0))
        last_seq = local_last_seq if incoming_last_seq is None else incoming_last_seq
        if last_seq != local_last_seq:
            raise SequenceConflict(
                "close last_seq 必须等于本地连续受理的最后序号",
                details={"local_last_seq": local_last_seq, "incoming_last_seq": last_seq},
            )

        duplicate = grant.get("close") is not None
        if not duplicate:
            grant["close"] = {"last_seq": last_seq, "closed_at": at}
            self._audit(at, "close_grant", grant=grant_id, last_seq=last_seq)
            self._save()
        return {"close": deepcopy(grant["close"]), "duplicate": duplicate,
                "balance": self.grant_balance(grant_id)}

    close = close_grant

    # ------------------------------------------------------------------ sync

    def pending_sync(self, grant_id: str | None = None) -> dict[str, list[dict[str, Any]]]:
        admissions: list[dict[str, Any]] = []
        receipts: list[dict[str, Any]] = []
        closes: list[dict[str, Any]] = []
        for record in self.state["requests"]:
            if grant_id is not None and record.get("grant_id") != grant_id:
                continue
            if not record.get("admission_synced"):
                admissions.append(self.admission_envelope(record["account"], record["request"]))
            if record["status"] in RequestStatus.TERMINAL and not record.get("receipt_synced"):
                receipts.append(self.receipt_envelope(record["account"], record["request"]))
        for grant in self.state["grants"]:
            if grant_id is not None and grant["grant_id"] != grant_id:
                continue
            if grant.get("close") and not grant["close"].get("synced"):
                closes.append(self.close_envelope(grant["grant_id"]))
        return {"admissions": admissions, "receipts": receipts, "closes": closes}

    def admission_envelope(self, account: str, request: str) -> dict[str, Any]:
        record = self.get_request(account, request)
        return {
            "account": record["account"],
            "site": self.site,
            "request": record["request"],
            "grant": record["grant_id"],
            "seq": record["seq"],
            "accepted_at": record["accepted_at"],
            "maximum": record["maximum"],
            "use": record.get("use"),
        }

    def receipt_envelope(self, account: str, request: str) -> dict[str, Any]:
        record = self.get_request(account, request)
        if record["status"] not in RequestStatus.TERMINAL:
            raise ValidationError("请求尚无最终回执", details={"request": request})
        return {
            "account": record["account"],
            "site": self.site,
            "request": record["request"],
            "result": record["status"],
            "used": record.get("used", 0),
        }

    def close_envelope(self, grant_id: str) -> dict[str, Any]:
        balance = self.grant_balance(grant_id)
        if not balance["close"]:
            raise ValidationError("grant 尚未本地收口", details={"grant": grant_id})
        return {
            "account": balance["account"],
            "site": self.site,
            "grant": grant_id,
            "last_seq": balance["last_seq"],
        }

    def mark_admission_synced(self, account: str, request: str, *, at: int | None = None) -> None:
        record = next(r for r in self.state["requests"] if r["account"] == account and r["request"] == request)
        record["admission_synced"] = True
        record["admission_synced_at"] = self._at(at)
        self._save()

    def mark_receipt_synced(self, account: str, request: str, *, at: int | None = None) -> None:
        record = next(r for r in self.state["requests"] if r["account"] == account and r["request"] == request)
        record["receipt_synced"] = True
        record["receipt_synced_at"] = self._at(at)
        self._save()

    def mark_close_synced(self, grant_id: str, *, at: int | None = None) -> None:
        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError(grant_id)
        if grant.get("close"):
            grant["close"]["synced"] = True
            grant["close"]["synced_at"] = self._at(at)
        self._save()

    def mark_center_settled(
        self,
        grant_id: str,
        *,
        returned_units: int,
        actual_used: int,
        at: int | None = None,
    ) -> dict[str, Any]:
        """Persist the center's settlement acknowledgement on the edge."""
        grant = self._find_grant(grant_id)
        if grant is None:
            raise NotFoundError(grant_id)
        grant["returned_units"] = returned_units
        grant["center_settlement"] = {
            "settled_at": self._at(at),
            "actual_used": actual_used,
            "returned_units": returned_units,
        }
        self._audit(self._at(at), "center_settled", grant=grant_id, returned=returned_units)
        self._save()
        return self.grant_balance(grant_id)
