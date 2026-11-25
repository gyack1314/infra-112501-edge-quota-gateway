"""Command line entry points for replay, demo and persisted reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from .central import CentralGateway
from .demo import run_demo
from .errors import GatewayError
from .http_server import serve
from .replay import replay_trace


def print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False))


def format_brief_report(report: dict[str, Any]) -> list[str]:
    balances = report["balances"]
    lines = [
        f"账户 {report['account']}/{report['period']}  cap={report['cap']}",
        "  实际用量={actual_used}  对 cap 占用={occupied_against_cap}  "
        " signed_headroom={headroom_signed}  新增可用={shared_new_business_available}".format(**balances),
        f"  在线未决预留={balances['online_pending_reserved']}  "
        f"未结 grant 本金={balances['open_grant_principal']}  "
        f"待收合同余量={balances['blocked_contractual_remainder']}  "
        f"当前可收={balances['reclaimable_units']}  "
        f"已收回余量={balances['reclaimed_units']}",
    ]
    if balances["over_capacity"]:
        lines.append(f"  !! 低于新上限的差额: {balances['over_capacity']}；新增占用已阻止")
    if report["unknown_requests"]:
        keys = ", ".join(f"{r['site']}/{r['request']}" for r in report["unknown_requests"])
        lines.append(f"  结果未知请求: {keys}")
    if report["unmatched_receipts"]:
        keys = ", ".join(f"{r['site']}/{r['request']} used={r.get('used')}" for r in report["unmatched_receipts"])
        lines.append(f"  尚待受理证明匹配的回执: {keys}")
    for missing in report["missing_records"]:
        lines.append(f"  缺口 grant={missing['grant']} site={missing['site']} missing_seq={missing['missing_seq']}")
    for grant in report["grants"]:
        lines.append(
            f"  grant {grant['grant']}/{grant['site']} status={grant['status']} "
            f"units={grant['units']} actual={grant['actual_used']} "
            f"pending={grant['pending_reserved']} returned={grant['returned_units']} "
            f"missing_seq={grant['missing_seq']}"
        )
    return lines


def command_replay(args: argparse.Namespace) -> int:
    result = replay_trace(args.trace, args.state_dir)
    if args.json:
        print_json(result)
        return 0

    print(f"回放 {result['trace']}（状态目录 {result['state_dir']}）")
    for event in result["events"]:
        if event["ok"]:
            print(f"  t{event['timestamp']:<3} #{event['index']:<2} {event['type']:<12} OK")
        else:
            error = event["error"]
            print(f"  t{event['timestamp']:<3} #{event['index']:<2} {event['type']:<12} {error['code']}: {error['message']}")
    for inspection in result["inspections"]:
        print(f"\n[inspect @ t{inspection['timestamp']}]")
        for line in format_brief_report(inspection["report"]):
            print(line)
    return 0


def command_demo(args: argparse.Namespace) -> int:
    result = run_demo(args.state_dir)
    for line in result["log"]:
        print("- " + line)
    print("\n最终对账：")
    central = result["report"]["central"]
    for line in format_brief_report(central):
        print(line)
    print("\n边缘授权余额：")
    for edge in result["report"]["edge_balances"]:
        print(
            f"  {edge['site']}/{edge['grant']} units={edge['units']} "
            f"actual={edge['actual_used']} occupied={edge['occupied']} "
            f"unspent={edge['unspent_balance']} spendable={edge['spendable_balance']} "
            f"returned={edge['returned_to_center']}"
        )
    print(f"\n持久化状态目录：{result['state_dir']}")
    if args.json:
        print_json(result)
    return 0


def command_report(args: argparse.Namespace) -> int:
    center = CentralGateway(args.state_dir)
    report = center.account_report(account=args.account, period=args.period)
    if args.json:
        print_json(report)
    else:
        for line in format_brief_report(report):
            print(line)
    return 0


def command_serve(args: argparse.Namespace) -> int:
    serve(role=args.role, state=args.state, site=args.site, host=args.host, port=args.port)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="中心/边缘 Token 限额网关与断网对账演练")
    sub = parser.add_subparsers(dest="command", required=True)

    replay = sub.add_parser("replay", help="按 data/*.jsonl 事件契约回放")
    replay.add_argument("trace", type=Path)
    replay.add_argument("--state-dir", type=Path, default=None)
    replay.add_argument("--json", action="store_true")
    replay.set_defaults(func=command_replay)

    demo = sub.add_parser("demo", help="运行内置断网、重启、重连、降配和对账演练")
    demo.add_argument("--state-dir", type=Path, default=None)
    demo.add_argument("--json", action="store_true")
    demo.set_defaults(func=command_demo)

    serve_parser = sub.add_parser("serve", help="以本机 HTTP JSON/RPC 进程运行中心或边缘网关")
    serve_parser.add_argument("--role", choices=["central", "edge"], required=True)
    serve_parser.add_argument("--state", type=Path, required=True)
    serve_parser.add_argument("--site", default=None, help="role=edge 时的站点标识")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.set_defaults(func=command_serve)

    report = sub.add_parser("report", help="读取持久化中心状态并输出账户/期间报告")
    report.add_argument("--state-dir", type=Path, required=True)
    report.add_argument("--account", required=True)
    report.add_argument("--period", required=True)
    report.add_argument("--json", action="store_true")
    report.set_defaults(func=command_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        argv = ["demo"]
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except GatewayError as error:
        print_json({"error": error.to_dict()})
        return 2
    except OSError as error:
        print(f"错误: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
