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
