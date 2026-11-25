# 断网站点的推理配额授权与回执结算网关

中心网关给在线与断连站点分配订阅额度。Token 为整数计量单位，测试驱动按照记录中的逻辑秒点推进，不接入真实推理端。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **limit**　额度控制端发布 account、period、cap、starts、ends。时间段含 starts、不含 ends。相同期间的新 cap 从该消息 timestamp 起生效；期间标识不因修改 cap 改变。
- **grant**　中心尝试给 site 授出 grant 标识、period、units 与有效窗口 [starts, ends)。份额不能跨期间。该中心请求中的 grant 是请求标识，供重试与上游回执归属使用。
- **admit**　在线网关或离线站点提交一次推理准入。grant 缺失代表在线准入，离线请求携带 grant 与该授予内从 1 开始的受理序号 seq，序号只给已经本地受理的请求使用。maximum 为 Token 上限。
- **finish**　仿真提供方对 account、site、request 出具最终回执。result 为 complete 或 never_started。complete 携带 used，范围为 0 到受理上限。timeout 事件只模拟调用端未收到答复。
- **sync_finish**　中心实际收到站点转交的提供方回执，语义与 finish 相同。
- **sync_admit**　中心收到离线受理证明，包含原受理时间 accepted_at。重连事件自身的 timestamp 是到达时间。
- **close**　站点提交份额已经关闭的声明，包含 last_seq 即最后一个现场获准受理的序号，0 表示从未受理。该站点在窗口截止后才能形成最终关闭声明。
- **inspect**　查询中心订阅账户或现场份额。

- **timeout**　调用方等待回执超出期限。引用 account、site、request，不代表提供方有终结结果。

## 资料文件

- unfinished-grant.jsonl
- period-boundary.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。

## 已实现的网关能力

仓库现在包含标准库实现的 `gateway` 包和 `test_gateway.py`：

- `CentralGateway`：订阅 cap/期间、共享额度、在线预留、超时状态、grant 授出、乱序回执/补单、收口缺口、降配差额和跨站点回收。
- `EdgeSite`：站点本地持久化 grant、连续 `seq`、受理即预留、回执释放差额、重试幂等、进程重启恢复和重连 outbox。
- `NetworkSimulation`：模拟断网、边缘重启、提供方仿真回执、重连后重复/乱序补报。
- JSONL replay：直接回放本目录两个演练事件文件。
- 可选 HTTP JSON/RPC 适配：可用同一套 Python 代码分别启动中心或边缘进程。

### 一键演练

```sh
python3 -m gateway demo
python3 -m gateway replay data/unfinished-grant.jsonl
python3 -m gateway replay data/period-boundary.jsonl
python3 -m unittest -v
```

`demo` 默认在系统临时目录写中心与边缘 JSON 状态，并在结尾打印路径；也可用 `--state-dir .demo-state` 指定可见目录，`--json` 输出完整结构化结果。

回放结果中：

- `admit` 无 `grant`：进入中心在线额度。
- `admit` 有 `grant`：进入对应边缘站点本地账本；超过 `[starts, ends)` 的新受理会被拒绝。
- `sync_finish` 可以先于 `sync_admit`：中心先持久化回执，等受理证明到达后匹配结算。
- `close.last_seq` 与已有 `seq` 对照后给出 `missing_seq`；缺口或未终结回执未消除前，grant 本金继续占用。
- 所有 `seq` 都有有效受理证明且每笔都有 `complete`/`never_started` 最终回执后，grant 才结清并把 `units - actual_used` 退回共享额度。

### 直接调用 Python API

```python
from gateway import CentralGateway, EdgeSite

center = CentralGateway("state/center.json")
bay = EdgeSite("bay-a", "state/edges/bay-a.json")

center.publish_limit(
    account="plant", period="march", cap=120, starts=0, ends=50, at=0
)
grant = {
    "account": "plant", "site": "bay-a", "grant": "grant-a",
    "period": "march", "units": 80, "starts": 1, "ends": 20,
}
center.issue_grant(grant, at=1)
bay.receive_grant(grant, at=1)

# 断网后只花本地 grant；重启 EdgeSite(...) 会从 JSON 恢复同一余额。
bay.admit_offline(
    account="plant", request="offline-a", grant="grant-a",
    seq=1, maximum=35, at=3,
)
bay.record_receipt(
    account="plant", request="offline-a",
    result="never_started", at=27,
)

# 重连后可先交回执，也可重复提交；中心按 account/site/request 幂等。
center.sync_finish(
    account="plant", site="bay-a", request="offline-a",
    result="never_started", at=27,
)
center.sync_admit(
    account="plant", site="bay-a", grant="grant-a", request="offline-a",
    seq=1, accepted_at=3, maximum=35, at=27,
)
print(center.account_report(account="plant", period="march"))
```

同一 `(account, site, request)` 重放完全相同的 `maximum/use/grant/seq` 时返回原记录；若上限或用途变化，抛出 `IdempotencyConflict`。客户端 `timeout` 只增加未知状态记录，不会释放预留；只有 `never_started` 或带最终 `used` 的 `complete` 才终结请求。

### 对账口径

`account_report()` 的核心字段位于 `balances`：

- `actual_used`：已有最终回执且归属到该期间的实际 Token。
- `online_pending_reserved`：在线未决请求的 `maximum` 预留。
- `open_grant_principal`：尚未结清的站点份额本金；断网补单缺口期间不提前拆解释放。
- `settled_grant_actual`：已结清 grant 转入实际用量口径的部分。
- `occupied_against_cap = online_actual + online_pending + open_grant_principal + settled_grant_actual`。
- `headroom_signed = cap - occupied_against_cap`；负数即降配后超出新上限的差额。
- `reclaimed_units`：已从 grant 退回共享额度的差额累计。

客服视图 `report["customer_service"]` 同时列出降配差额、结果未知/超时请求、尚未匹配受理证明的最终回执，以及仍被占住且带 blocker 的站点 grant。边缘的 `grant_balance()` 另外给出 `spendable_balance`、`unspent_balance` 和 `pending_return_to_center`；断网可接单余额以边缘持久化账本为准，而不是账户缓存总额或上次上报余额。

### 可选 HTTP 进程

```sh
python3 -m gateway serve --role central --state .runtime/center.json --port 8765
python3 -m gateway serve --role edge --site bay-a --state .runtime/bay-a.json --port 8766
```

调用方式：

```sh
curl -s http://127.0.0.1:8765/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"method":"account_report","params":{"account":"plant","period":"march"}}'
```

HTTP 仅默认绑定本机回环地址，供本地演练和测试使用。
