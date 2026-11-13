# 银行入账 × 内部收款流水核对账服务

财务数据工程师用的对账明细系统：上传**银行入账**与**内部收款**两类 CSV，
后台枚举全部候选组合（1 条银行流水对应 1～3 条内部流水：户名相同、每条内部
流水与银行流水日期相差不超过 2 日、金额合计相等、且均未匹配），核对员确认
组合后可查询对应关系与未匹配明细，用于排查漏记与重复记账。

批量更正前可使用**两阶段暂存导入**：先预审一侧 CSV（保存批次、逐条标注
新增/变更/不变、列出待撤销整组成员原值，不动正式流水），按批次号确认后才
整批应用，预审失效会整批拒绝并列出冲突。

技术栈：**FastAPI + SQLite（WAL）+ Uvicorn + Docker Compose**。

---

## 1. 快速启动

```bash
docker compose up --build
```

启动后：

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 交互式控制台 | <http://localhost:8000/> | 上传 CSV、查看候选、确认组合、查询结果 |
| Swagger / OpenAPI | <http://localhost:8000/docs> | 可直接 Try it out 的 API 文档 |
| 健康检查 | <http://localhost:8000/api/health> | 容器 HEALTHCHECK |

数据持久化在命名卷 `reconcile-data`（容器内 `/data/reconcile.db`），
容器重启或重建后已确认匹配不丢失。

本地无 Docker 时（需要 Python 3.11）：

```bash
pip install -r requirements.txt
RECONCILE_DB=./data/reconcile.db uvicorn app.main:app --reload
```

## 2. CSV 格式

每侧最多 **100 条**，UTF-8 编码，表头支持中英文：

| 列 | 中文表头 | 英文别名 | 约束 |
| --- | --- | --- | --- |
| 来源内唯一流水号 | 流水号 / 唯一流水号 | serial / id | 文件内不可重复 |
| 户名 | 户名 / 账户名 | name / account | 非空 |
| 日期 | 日期 / 交易日期 | date | `YYYY-MM-DD`（也接受 `YYYY/MM/DD`） |
| 金额 | 金额(分) | amount / cents | **正整数，单位：分** |

示例见 [`data/sample_bank.csv`](data/sample_bank.csv)、
[`data/sample_internal.csv`](data/sample_internal.csv)。

## 3. API 一览

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/uploads/{bank\|internal}` | 上传一侧 CSV（multipart 字段 `file`），即时生效 |
| GET | `/api/flows/{bank\|internal}` | 全部原始流水（含 `status`、`group_id`、`version`） |
| GET | `/api/candidates` | 全部可确认候选（`?include_stale=true` 可看过时项） |
| POST | `/api/candidates/{id}/confirm` | 确认组合，body 可带页面 `expected_versions` |
| GET | `/api/matches` | 已确认对应关系列表 |
| GET | `/api/matches/{group_id}` | 单组对应关系 |
| GET | `/api/unmatched` | 两侧未匹配明细 |
| POST | `/api/recompute` | 同步强制重算候选（上传后默认后台自动重算） |
| POST | `/api/imports/{bank\|internal}/preview` | **暂存预审**：保存批次，不动正式流水 |
| GET | `/api/imports` | 全部暂存批次（状态汇总） |
| GET | `/api/imports/{batch_id}` | 单批次完整明细（行判定/待撤销组/冲突/回执） |
| POST | `/api/imports/{batch_id}/confirm` | **整批确认**：复核冲突后原子应用，重放返回首次回执 |

### 典型流程

```bash
curl -F "file=@data/sample_bank.csv"      http://localhost:8000/api/uploads/bank
curl -F "file=@data/sample_internal.csv"  http://localhost:8000/api/uploads/internal
curl -X POST http://localhost:8000/api/recompute
curl        http://localhost:8000/api/candidates
curl -X POST http://localhost:8000/api/candidates/1/confirm \
     -H 'Content-Type: application/json' \
     -d '{"expected_versions":{"B002":1,"I004":1,"I005":1}}'
curl        http://localhost:8000/api/matches
curl        http://localhost:8000/api/unmatched
```

## 4. 关键语义如何保证

### 同号覆盖、同内容重传不变

每条流水保存内容哈希 `content_hash`。流水号相同即 `UPSERT` 覆盖原始值；
若哈希一致则整行跳过——不产生 version 跳动、不撤销任何匹配，重传结果幂等。

### 计算不占正式写事务，期间仍可上传/确认

候选枚举（含子集和搜索）在**后台线程**执行：先用一个短只读事务快照两侧
流水，在普通 Python 对象上完成全部计算，再用一个短 `BEGIN IMMEDIATE`
事务整体替换候选表。上传与确认走各自独立的写事务，计算过程不持有写锁。
上传后通过防抖（debounce）合并连续重算请求；`POST /api/recompute`
可同步重算。

### 并发确认只有一个成功；旧页面必须核对最新值

* 每条流水有触发器维护的单调递增 `version`，任何内容/状态变化都会 +1。
* 候选快照保存银行与各内部流水的 version（`expected_versions`）。
* 确认在单个 `BEGIN IMMEDIATE` 事务内重新校验：
  候选未过时 → 页面 version 与快照一致 → 快照 version 与**当前行**一致 →
  全部仍为 `unmatched` → 业务规则（同名/±2 日/金额合计）仍成立。
* 共用流水的两个确认串行化后，后到者发现流水已 `matched`，返回 **409**，
  并在响应 `detail.latest` 中回传最新原始值与认领状态，供旧页面刷新。

### 迟到计算不能把过时组合标为可确认

更正/撤销发生时，受影响流水涉及的候选行在**同一个写事务**内立即删除；
后台重算只会按当前未匹配流水重建候选。即使一份迟到的重算结果落库，确认
时的 version 比对也会拒绝基于旧快照的组合（409）。

### 更正已匹配流水 → 整组撤销

更正带 `group_id` 的流水时，先在同一事务内解散整个匹配组：组内银行与
内部流水全部回到 `unmatched`、删除匹配组、删除相关候选，再写入更正后的
原始值。**原始更正、撤销与状态变化一起提交**，不存在中间态。

### 失败/重启不留单边匹配，已确认数据保留

匹配组的插入、两侧流水状态翻转在单事务内完成，崩溃时事务整体回滚，
不可能出现一侧 matched、另一侧 unmatched。提交后的数据落入 WAL 并
checkpoint 到持久化卷，进程/容器重启后 `/api/matches` 完整保留。

## 4a. 暂存批量更正（两阶段导入）

面向财务数据工程师批量更正流水 CSV 的场景，在**不改动正式流水**的前提下
先核对覆盖范围与会被撤销的对账关系，再决定是否整批应用。原有
`/api/uploads/*` 即时上传 API 完全保留，两套入口可并行使用。

### 阶段一：预审 `POST /api/imports/{side}/preview`

接收一侧**原格式 CSV**（与即时上传同一解析器、同一 ≤100 条容量校验），
只写暂存表，正式流水、匹配组、候选均不发生任何变化。响应包含：

* `batch_id`（批次号）、`status=staged`、侧别与文件名；
* 逐条 `rows`，每条标注 `change_type`：
  * `added` 新增——该流水号当前不存在；
  * `changed` 变更——流水号存在但内容哈希不同；
  * `unchanged` 不变——内容哈希一致（确认时对该行零作用）；
* `disband_groups`：确认时将被**整组撤销**的匹配组，含组内银行/内部
  全部成员的**原值**（金额、状态、version、group_id）；
* `watched`：全部受监视流水（文件涉及的每个流水号，含预审时不存在者，
  以及每个待撤销组的全部成员）及其快照。

### 阶段二：确认 `POST /api/imports/{batch_id}/confirm`

确认在串行写事务内先复核全部受监视流水，再通过与即时上传**同一个
应用核心**（容量、同内容重传不变、整组撤销）整批落库：

* **冲突整批拒绝**（HTTP 409，批次置终态 `rejected` 并持久化 `conflicts`）：
  预审后，文件涉及的任一流水（含原先不存在者）或待撤销组任一成员的
  **原值（内容哈希/version）、认领状态（status）、组成员（group_id）**
  发生变化即拒绝，冲突逐条给出 `reason`（`flow_changed` /
  `status_changed` / `membership_changed` / `flow_appeared` /
  `flow_missing` / `capacity_exceeded`）与预审值、当前值；
* **无关变化不失效**：不在监视集合内的流水变化不影响确认；仅当无关新增
  导致应用后超过每侧 100 条容量时，才以 `capacity_exceeded` 冲突拒绝；
* **原子生效、不留半批**：成功回执 `receipt`、批次状态与全部业务改动
  （原始更正、整组撤销、状态翻转、候选失效）在同一事务提交；失败或中断
  整体回滚；
* **重启可继续**：暂存批次与正式数据在同一个持久化 SQLite 库中，进程
  重启后 `staged` 批次仍可取回并确认；
* **重放幂等**：并发确认或响应丢失后重试同一批次，已 `confirmed` 的批次
  直接返回**首次冻结回执**，绝不二次应用、绝不覆盖后来的更正；
  `rejected` 为终态，再次确认返回 409，需重新预审生成新批次。

预审等待确认期间，原上传、候选确认等所有 API 照常可用。

### 典型流程

```bash
# 1) 预审（正式流水不变），拿到 batch_id 与逐条判定/待撤销组原值
curl -F "file=@correction_bank.csv" \
     http://localhost:8000/api/imports/bank/preview

# 2) 随时查看批次完整明细
curl http://localhost:8000/api/imports/$BATCH_ID

# 3) 整批确认；冲突时返回 409 与 conflicts，成功返回 receipt
curl -X POST http://localhost:8000/api/imports/$BATCH_ID/confirm

# 4) 响应丢失后重试：返回同一份首次回执，不会重复应用
curl -X POST http://localhost:8000/api/imports/$BATCH_ID/confirm
```

## 5. 数据模型（SQLite）

```text
bank_flows / internal_flows   流水主表（version 触发器、content_hash、status、group_id）
match_groups                  已确认匹配组
candidates                    候选组合（candidate_key 稳定身份、version 快照、stale、generation）
candidate_items               候选 ↔ 流水成员关系
meta                          候选代数 generation
import_batches                暂存批次（staged/confirmed/rejected、计数、冲突 JSON、冻结回执 JSON）
import_batch_rows             批次内逐行暂存内容与 added/changed/unchanged 判定
import_batch_watch            受监视流水快照（含待撤销组成员、预审时不存在的流水号）
```

## 6. 测试

**无需任何第三方依赖**的核心测试（真实 SQLite，覆盖并发、重启与暂存导入
的全部冲突/幂等语义）：

```bash
python3 -I tests/test_core.py          # 原有对账核心（19 项）
python3 -I tests/test_staged_core.py   # 暂存两阶段导入核心（18 项）
```

端到端业务演示（同样只用标准库，含暂存预审→冲突拒绝→整批确认→重启续确认）：

```bash
python3 -I scripts/demo.py
```

容器/虚拟环境内安装依赖后可跑 HTTP 层测试：

```bash
pip install -r requirements.txt
pytest -q
```

## 7. 目录结构

```text
app/
  db.py                SQLite 连接、WAL、串行写事务、schema 与 version 触发器
  models.py            领域模型与错误类型（含批次状态与冲突错误）
  csv_parser.py        CSV 解析与校验（≤100 条、正整数分金额）
  matching.py          纯函数候选枚举（1~3 条、同名、±2 日、子集和）
  recompute.py         候选持久化 + 后台防抖重算线程
  ledger.py            上传/同号覆盖/幂等/整组撤销（暂存确认复用同一应用核心）
  matching_service.py  确认（乐观并发+最新值复核）与查询
  staged_import.py     暂存预审、确认冲突复核、整批原子应用、回执冻结与批次查询
  main.py              FastAPI 路由（原 API + 暂存导入 API）
static/index.html      交互式控制台（含暂存批量更正面板）
tests/                 核心测试（stdlib）与 API 测试（pytest）
scripts/demo.py        端到端演示脚本
data/                  示例 CSV
docker-compose.yml / Dockerfile / requirements.txt
```
