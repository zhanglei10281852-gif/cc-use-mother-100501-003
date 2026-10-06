# 跨节点算力预留与履约

跨节点算力预留与履约后端：节点发布容量批次，租户先取得可复核报价，再以幂等请求锁定配额并提交带依赖关系的任务组；平台在准入时防止超卖，在预留到期、节点降级、任务部分完成时按合同规则续约、迁移或释放；消费事件收敛为唯一账单，租户凭 API 或 CLI 即可核对从报价、占用到结算的完整履约轨迹。

仅依赖 Python 标准库（SQLite 嵌入式持久化），不需要外部数据库或其他运行服务。

## 运行环境

- Python 3.11 或更高版本
- Linux、macOS 或 Windows

## 快速开始

```bash
# 运行测试（59 个用例）
PYTHONPATH=src python3 -m unittest discover -s tests -v

# 编译检查
python3 -m compileall -q src tests run_cli.py

# 基础契约冒烟
python3 run_cli.py

# 端到端内存演示（报价 -> 预留 -> 任务组 -> 用量 -> 账单 -> 轨迹）
python3 run_cli.py --demo
```

## 核心设计

### 防超卖

- 每个容量批次对应一行容量台账（`capacity_ledger`），预留锁定在 `BEGIN IMMEDIATE` 事务内以条件更新完成，容量不足时整单拒绝；
- 数据库触发器 `no_oversell` 兜底，任何代码路径都不可能让分配量超过批次总量；
- 节点发布批次时可携带 `inventory_ref`（物理库存引用），平台级唯一约束使同一批加速卡不会被两个节点先后上架确认；
- 并发测试（8 线程争抢 4 张卡）验证只有一笔预留成功。

### 可复核报价

- 报价价格完全由输入决定：批次单价 × 能耗等级因子（P1=0.9 / P2=1.0 / P3=1.25）×（1 + 5% × 匹配服务能力数）× 卡数 × 时长；
- 报价落库时保存批次快照、计价明细与指纹，`GET /v1/quotes/{id}/verify` 或 `verify-quote` 可随时重算比对；
- 报价有有效期，被消费、过期或对应批次降级后不可再用于锁定。

### 幂等锁定

- `POST /v1/reservations` 必须携带 `Idempotency-Key`；
- 同键同载荷：返回首次创建的预留（`replayed: true`），不重复占用容量，重启后依然有效；
- 同键异载荷：返回 `409 idempotency_conflict`。

### 单计费点与迁移

- 每个预留在同一时刻至多一条开放计费段（`billing_segments` 上的部分唯一索引），从存储层杜绝同一任务在两处同时计费；
- 迁移切换在同一事务内完成：关闭旧段（`end_at=切换时刻`）、开启新段（`start_at=切换时刻`）、台账搬移，半开区间首尾相接；
- 迟到事件按用量时间窗归属旧段，乱序到达不影响归属；
- 迁移分 PLANNED / COMPLETED 两阶段落库，进程在两者之间崩溃时，重启后由恢复流程继续执行。

### 生命周期合同规则

预留到期且仍有未完成任务时，按顺序决策：

1. `auto_renew` 且续约次数未用尽且原批次仍可用 → 续约（按当前批次价重算费率并轮换计费段）；
2. `migratable` → 迁移到健康批次（节点降级时强制不同故障域），`end_at` 顺延 `extension_seconds`；
3. 否则 → 到期释放，未完成任务标记为中断。

其他触发点：

- 节点降级（`degrade-node`）：批次置为 DEGRADED、未消耗报价作废；可迁移预留迁往其他故障域，不可迁移的进入抢占流程；
- 任务部分完成且 `shrink_on_partial`：按在途任务需求收缩配额，关闭旧段开启新段，台账同步释放；
- 批次有效期届满：批次退役并疏散其上预留（迁移或抢占）。

### 抢占与恢复

- 抢占记录（`preemptions`）完整保存受影响租户、每预留补偿金额与恢复次序；
- 补偿 = 剩余时长价值 × 1.5，作为负金额行进入覆盖抢占时刻的账单；
- 恢复次序按租户优先级（数值小者优先）与创建时间排列，`restore-preemption` 严格按序重新接纳，容量不足时保持 RESTORING 状态等待。

### 消费事件收敛为唯一账单

- 事件按 `event_id` 去重：重复到达幂等忽略，同 ID 异载荷报冲突；
- 账单由（预留、账期）唯一确定，重复生成返回同一账单同一指纹；
- 账单 = 各计费段占用行 + 超量行（计量超出预留配额部分）+ 抢占补偿行；指纹基于归一化明细，同样输入永远得到同样指纹；
- 终审（FINALIZED）后又有迟到事件：生成 ADJUSTMENT 调整单记录差额，历史账单不被篡改。

### 重启恢复

`Backend.recover()`（服务启动时自动执行，也可经 `POST /v1/admin/recover` 或 `recover` 命令触发）：

1. 续跑所有 PLANNED 状态的迁移（执行幂等）；
2. 补跑到期清扫（报价过期、批次退役、预留续约/迁移/释放）。

全部状态在 SQLite 中，恢复流程可重复执行而不产生副作用。

## HTTP API

启动服务：

```bash
PYTHONPATH=src python3 -m compute_reservation.cli --db compute.db serve --host 127.0.0.1 --port 8080
```

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/nodes/{node}/batches` | 发布容量批次（有效期、故障域、能耗等级、服务能力、单价、库存引用） |
| `GET /v1/batches` | 列出批次与剩余容量 |
| `POST /v1/quotes` | 申请可复核报价 |
| `GET /v1/quotes/{id}` / `GET /v1/quotes/{id}/verify` | 查看 / 复核报价 |
| `POST /v1/reservations` | 幂等锁定配额（需 `Idempotency-Key` 头） |
| `GET /v1/reservations` / `GET /v1/reservations/{id}` | 查看预留、计费段与任务 |
| `POST /v1/reservations/{id}/renew` / `release` | 手动续约 / 释放 |
| `POST /v1/task-groups` | 提交带依赖的任务组 |
| `POST /v1/tasks/{id}/complete` | 标记任务完成（触发依赖推进与配额收缩） |
| `POST /v1/nodes/{id}/degrade` / `restore` | 节点降级 / 恢复 |
| `POST /v1/preemptions` / `POST /v1/preemptions/{id}/restore` | 抢占 / 按序恢复 |
| `POST /v1/usage-events` | 批量摄入消费事件 |
| `POST /v1/bills` / `GET /v1/bills/{id}` | 生成（可终审）/ 查看账单 |
| `GET /v1/tenants/{id}/trail` | 核对完整履约轨迹 |
| `POST /v1/admin/sweep` / `POST /v1/admin/recover` | 到期清扫 / 恢复 |

租户身份经 `X-Tenant-Id` 头（或请求体 `tenant_id`）提供，跨租户访问返回 403。错误统一为 `{"error": {"code", "message"}}`。

## 命令行

所有命令支持 `--db` 指定数据库文件（默认 `compute.db`），时间参数接受 Unix 秒或 ISO-8601：

```bash
CLI="python3 -m compute_reservation.cli --db compute.db"
PYTHONPATH=src $CLI publish-batch --node node-a --cards 8 --valid-from 1790000000 --valid-until 1790086400 \
    --fault-domain fd-1 --energy-tier P2 --capabilities training --price 10 --inventory-ref inv-001
PYTHONPATH=src $CLI quote --tenant tenant-1 --cards 2 --start 1790000000 --duration 3600 --capabilities training
PYTHONPATH=src $CLI verify-quote --quote quo_xxx
PYTHONPATH=src $CLI reserve --tenant tenant-1 --quote quo_xxx --idem-key k-1 --auto-renew --max-renewals 1
PYTHONPATH=src $CLI submit-group --tenant tenant-1 --reservation res_xxx --file tasks.json
PYTHONPATH=src $CLI complete-task --tenant tenant-1 --task tsk_xxx
PYTHONPATH=src $CLI degrade-node --node node-a --reason "温度告警"
PYTHONPATH=src $CLI ingest-usage --file events.json
PYTHONPATH=src $CLI bill --tenant tenant-1 --reservation res_xxx --from 1790000000 --to 1790003600 --finalize
PYTHONPATH=src $CLI trail --tenant tenant-1        # 从报价、占用到结算的完整轨迹
PYTHONPATH=src $CLI recover                        # 重启恢复：未决迁移 + 到期清扫
```

## 目录结构

```
src/compute_reservation/
  contracts.py   # 基础领域契约（稳定标识、不可变版本、内容摘要）
  models.py      # 状态常量与公共辅助
  pricing.py     # 确定性定价与补偿规则
  store.py       # SQLite 持久化：事务、台账触发器、单开放段索引、审计日志
  services.py    # 目录、报价、幂等预留、任务组调度
  lifecycle.py   # 到期续约/迁移/释放、节点降级、抢占与恢复
  billing.py     # 事件摄入去重、账单收敛、履约轨迹
  recovery.py    # 重启恢复入口
  backend.py     # 服务装配门面
  api.py         # HTTP API（标准库实现）
  cli.py         # 命令行入口
tests/           # 59 个单元与端到端用例
```
