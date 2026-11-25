"""End-to-end in-process rehearsal used by ``python -m gateway demo``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .simulation import NetworkSimulation


def run_demo(state_dir: str | Path | None = None) -> dict[str, Any]:
    sim = NetworkSimulation(state_dir)
    log: list[str] = []

    def say(message: str) -> None:
        log.append(message)

    sim.publish_limit(
        account="plant", period="march", cap=120, starts=0, ends=50, at=0
    )
    sim.connect("bay-a")
    sim.connect("cloud")
    sim.issue_grant(
        account="plant",
        site="bay-a",
        grant="grant-a",
        period="march",
        units=80,
        starts=1,
        ends=20,
        at=1,
    )
    say("中心发布 cap=120，并向 bay-a 授出 80 Token 的时间窗份额")

    sim.disconnect("bay-a")
    sim.offline_admit(
        site="bay-a", account="plant", request="offline-a",
        grant="grant-a", seq=1, maximum=35, use="vision", at=3,
    )
    sim.offline_admit(
        site="bay-a", account="plant", request="offline-b",
        grant="grant-a", seq=2, maximum=30, use="guide", at=4,
    )
    say("bay-a 断网后连续受理两笔，本地剩余授权=15")

    sim.restart_edge("bay-a")
    retry = sim.offline_admit(
        site="bay-a", account="plant", request="offline-b",
        grant="grant-a", seq=2, maximum=30, use="guide", at=4,
    )
    assert retry["duplicate"] is True
    assert sim.edge("bay-a").grant_balance("grant-a")["available"] == 15
    say("边缘进程重启后从持久化账本恢复；原请求重试未重复占额")

    sim.online_admit(
        account="plant", site="cloud", request="online-a", maximum=25, use="assist", at=5
    )
    sim.timeout_online(account="plant", site="cloud", request="online-a", at=6)
    sim.restart_center()
    unknown = sim.center.get_request("plant", "cloud", "online-a")
    assert unknown["result_unknown"] is True
    assert sim.center.period_totals("plant", "march")["online_pending_reserved"] == 25
    say("cloud 调用超时且中心重启；请求仍可单独查询，25 Token 未释放")

    sim.close_edge_grant(
        site="bay-a", account="plant", grant="grant-a", last_seq=2, at=21
    )
    sim.center.receive_close(
        sim.edge("bay-a").close_envelope("grant-a") | {"at": 21}
    )
    sim.edge("bay-a").mark_close_synced("grant-a", at=21)
    say("窗口截止后站点收口 last_seq=2；中心指出 seq 1、2 受理证明缺口，保留 80 承诺")

    sim.provider.finish_edge(
        site="bay-a", account="plant", request="offline-b",
        result="complete", used=12, at=22,
    )
    receipt_envelope = sim.edge("bay-a").receipt_envelope("plant", "offline-b")
    sim.center.sync_finish({**receipt_envelope, "at": 22})
    sim.edge("bay-a").mark_receipt_synced("plant", "offline-b", at=22)
    say("offline-b 回执先于受理证明到达中心；中心隔离匹配，不提前放开 grant")

    admit_a = sim.edge("bay-a").admission_envelope("plant", "offline-a")
    sim.center.sync_admit({**admit_a, "at": 23})
    sim.edge("bay-a").mark_admission_synced("plant", "offline-a", at=23)

    sim.publish_limit(
        account="plant", period="march", cap=75, starts=0, ends=50, at=25
    )
    service = sim.center.customer_service_view("plant", "march")
    assert service["difference_above_new_cap"] == 30
    say("班中 cap 调到 75；既有占用 105，超出 30，新增占用停止，客服视图可查未决项和被占份额")

    admit_b = sim.edge("bay-a").admission_envelope("plant", "offline-b")
    sim.center.sync_admit({**admit_b, "at": 26})
    sim.edge("bay-a").mark_admission_synced("plant", "offline-b", at=26)

    sim.provider.finish_edge(
        site="bay-a", account="plant", request="offline-a",
        result="never_started", used=0, at=27,
    )
    receipt_a = sim.edge("bay-a").receipt_envelope("plant", "offline-a")
    grant_report = sim.center.sync_finish({**receipt_a, "at": 27})["grant"]
    sim.edge("bay-a").mark_receipt_synced("plant", "offline-a", at=27)
    assert grant_report["status"] == "settled"
    assert grant_report["returned_units"] == 68
    sim.edge("bay-a").mark_center_settled(
        "grant-a", returned_units=68, actual_used=12, at=27
    )
    say("seq 与回执补齐后 grant 结清：实际 12，68 余量回到共享额度")

    sim.center.receive_finish(
        account="plant", site="cloud", request="online-a",
        result="complete", used=22, at=28,
    )
    report = sim.reconciliation_report("plant", "march")
    balances = report["central"]["balances"]
    assert balances["actual_used"] == 34
    assert balances["headroom_signed"] == 41
    assert balances["reclaimed_units"] == 68
    say("online-a 实际 22，释放 3；账户实际用量 34，当前可用上限空间 41")

    return {
        "state_dir": str(sim.root),
        "log": log,
        "report": report,
        "customer_service_at_cap_change": service,
    }
