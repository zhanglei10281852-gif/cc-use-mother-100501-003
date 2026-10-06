# 跨节点算力预留与履约

面向统一算力交易节点交易入口的后端：节点发布容量批次，租户先取得可复核报价，再以幂等请求锁定配额并提交具有依赖关系的任务组；平台按合同规则处理到期续约、节点降级迁移与部分完成释放，消费事件收敛为唯一账单。纯 Python 标准库实现（SQLite 持久化 + 内置 HTTP API + CLI），不依赖外部服务。

## 运行环境

- Python 3.11 或更高版本
- Linux、macOS 或 Windows

## 运行测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译检查

```bash
python3 -m compileall -q src tests run_cli.py
```

## 快速开始

```bash
# 端到端演练：报价 → 幂等锁定 → 任务组 → 节点降级迁移 → 结算 → 履约轨迹
python3 run_cli.py --db /tmp/cr.db demo

# 启动 HTTP API（后台自动执行过期回收与未决迁移续跑）
python3 run_cli.py --db /tmp/cr.db serve --port 8080
```

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 节点（node） | 算力节点，携带故障域、能耗等级、服务能力，状态 ACTIVE/DEGRADED/OFFLINE |
| 容量批次（capacity batch） | 节点发布的可售容量，带有效期；同节点同资源类型的 OPEN 批次时间窗不得重叠 |
| 报价（quote） | 可复核的价格快照：冻结批次可用量、费率卡与金额摘要，可重算校验，只能被消费一次 |
| 预留（reservation） | 幂等锁定的配额，冻结合同规则（到期/降级/抢占/部分完成策略、优先级、补偿比例） |
| 占用区间（segment） | 预留的计费单元，迁移/收缩时切换，区间连续不重叠，杜绝重复计费 |
| 任务组（task group） | 具有依赖关系（DAG）的任务集合，事件驱动状态机 |
| 迁移（migration） | PLANNED → COMMITTED/ABORTED；提交时原子切换占用区间与配额 |
| 抢占（preemption） | 持久化受影响租户、补偿贷记与恢复次序，容量恢复后按序回迁 |
| 账单（bill） | 结算差额入账后签发；同一状态重复结算收敛为唯一账单 |

## 关键不变量

- **防超卖**：准入即 `UPDATE ... WHERE allocated + ? <= total`，在独占写事务内原子生效；并发测试证明 8 线程抢 4 单元只有 4 个成功。
- **幂等锁定**：`idempotency_key` 唯一；同键同体重放返回原预留（`idempotent_replay: true`），同键不同体返回 `idempotency_conflict`。
- **报价单赢家**：`UPDATE quotes ... WHERE state='OPEN'` 保证并发消费同一报价只有一个成功，从机制上杜绝"两个节点先后确认同一批卡"。
- **迁移不重复计费**：切换点同时关闭旧区间、开启新区间（同一事务），账单按区间求和，总额恒等于连续占用时长 × 单价。
- **事件收敛**：消费事件按 `event_id` 去重；乱序、重复、迟到的事件通过"应收总额 − 已入账"差额入账收敛，迟到事件只产生增量账单。
- **重启续跑**：全部状态在 SQLite，`sweep` 幂等续跑未决迁移、到期回收、降级重试与抢占回迁。

## 合同规则（预留时冻结，可逐项覆盖）

```json
{
  "on_expiry": "renew | migrate | release",
  "renew_extension_hours": 12,
  "on_degrade": "migrate | keep",
  "on_partial": "shrink | keep",
  "on_complete": "release | keep",
  "on_preempt": "migrate | release",
  "interruptible": true,
  "priority": 100,
  "required_capabilities": ["rdma"],
  "compensation": {"capacity_loss": 0.5, "forced_migration": 0.2, "forced_expiry": 0.1}
}
```

- 到期（`sweep` 触发）：`renew` 在批次有效期内自动续约，续约不成退为迁移；`migrate` 寻找匹配批次做可中断迁移并顺延；`release` 直接释放。非自愿到期按 `forced_expiry` 补偿。
- 节点降级（DEGRADED）：`migrate` 合同的预留自动迁往其他故障域的健康节点，并按 `forced_migration` 贷记补偿；无目标时挂起，后续 sweep 重试。
- 节点离线（OFFLINE）：在库预留被抢占（PREEMPTED），按 `capacity_loss` 补偿并记录恢复次序；容量恢复后 `recovery`/`sweep` 按序回迁。
- 任务部分完成：`shrink` 按剩余任务最大需求收缩配额并切分占用区间；全部完成后 `release` 自动释放并出账。

## CLI 速查

```bash
PY="python3 run_cli.py --db /tmp/cr.db"
$PY node-register --name 华东A --fault-domain room-a --energy-level medium --capabilities rdma,nvlink
$PY batch-publish --node NB-000001 --resource-type gpu --units 8 --from 2026-10-06T00:00:00Z --until 2026-10-09T00:00:00Z
$PY quote --tenant tenant-lab --batch CB-000001 --units 4 --start 2026-10-06T18:00:00Z --end 2026-10-08T00:00:00Z
$PY quote-verify QT-000001
$PY lock --idempotency-key lab-001 --quote QT-000001 --contract '{"on_expiry":"migrate"}'
$PY task-group-submit --tenant tenant-lab --name night-train --spec tasks.json
$PY events --file events.json            # 消费事件，重复 event_id 自动去重
$PY node-state NB-000001 --state DEGRADED # 触发降级迁移
$PY sweep                                 # 过期回收 + 未决迁移续跑 + 抢占回迁
$PY settle --tenant tenant-lab            # 幂等结算
$PY trail --tenant tenant-lab             # 报价→占用→结算完整履约轨迹
```

所有命令输出 JSON；业务冲突以退出码 2 与稳定错误码（如 `capacity_insufficient`、`idempotency_conflict`）返回。`--now` 可固定时钟用于可复现演练。

## HTTP API

`serve` 启动后提供等价资源（均返回 JSON，错误为 `{"error": {"code", "message"}}`）：

- 容量：`POST/GET /v1/nodes`、`POST /v1/nodes/{code}/state`、`POST/GET /v1/batches`、`POST /v1/batches/{code}/close`
- 交易：`POST /v1/quotes`、`POST /v1/quotes/{code}/verify`、`POST /v1/reservations`、`POST /v1/reservations/{code}/renew|release`
- 任务与事件：`POST /v1/task-groups`、`GET /v1/task-groups/{code}`、`POST /v1/events`
- 履约：`POST /v1/migrations`、`POST /v1/migrations/{code}/commit|abort`、`POST /v1/preemptions`、`POST /v1/recovery`、`POST /v1/sweep`
- 结算：`POST /v1/settlements`、`GET /v1/bills`、`GET /v1/bills/{code}`、`GET /v1/reservations/{code}/ledger`
- 核对：`GET /v1/tenants/{code}/trail`、`GET /v1/audit`（`X-Actor` 请求头写入审计归属）

## 项目结构

```
src/compute_reservation/
  contracts.py  # 初始领域契约（不可变对象、稳定摘要、冲突检测）
  models.py     # 状态机常量、合同默认值、时间/金额工具、DomainError
  store.py      # SQLite 模式、独占写事务、单调业务编码
  system.py     # 领域服务门面：全部业务不变量在此强制
  api.py        # 标准库 HTTP JSON API
  cli.py        # 命令行入口与端到端演练
tests/          # 31 个测试：契约、领域行为、并发、API、CLI
run_cli.py      # 仓库根入口（默认 smoke，子命令见 --help）
```
