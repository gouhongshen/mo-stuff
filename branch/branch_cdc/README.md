# MatrixOne Branch CDC 数据同步工具 v4.1

基于 MatrixOne **Data Branch** 特性的数据同步工具，支持将上游数据库/表通过 PITR 快照机制增量或全量同步到下游数据库。

## 快速开始

### 1. 安装依赖

```bash
pip install pymysql questionary rich
```

### 2. 三步启动同步

```bash
# 第一步：直接运行，首次会自动进入配置向导
python branch_cdc.py

# 第二步：在交互菜单中完成配置（上下游连接、Stage、PITR）
# 第三步：选择 "Manual Sync Now" 执行首次同步
```

首次运行时，工具会自动引导你完成所有配置，无需手动编写 config.json。

### 3. 配置向导详解

首次运行（或无 config.json 时），工具自动进入交互式配置向导，按以下顺序完成：

#### Step 1: 选择同步粒度（Sync Scope）

```
? Action: Edit Sync Scope
? Sync Scope: (选择 table 或 database)
```

- **table** — 同步单张表，需指定上下游的具体表名
- **database** — 同步整个数据库，上游库下所有表自动同步到下游库（表名用 `*` 表示）

#### Step 2: 配置上游连接（Upstream）

```
? Action: Edit Upstream
? Host: 127.0.0.1
? Port: 6001
? User: dump
? Pass: ****
? DB:   tpcc
? Table: bmsql_stock    # database 模式下不需要填
```

#### Step 3: 配置下游连接（Downstream）

```
? Action: Edit Downstream
? Host: 127.0.0.1
? Port: 6001
? User: dump
? Pass: ****
? DB:   tpcc_ds
? Table: bmsql_stock    # database 模式下不需要填
```

> 下游数据库和表如果不存在，工具会自动创建。

#### Step 4: 配置 Stage

```
? Action: Edit Stage
? Stage Name: stage02
```

Stage 是 MatrixOne 的数据中转存储，用于存放 diff 产生的文件。需要在 MatrixOne 中预先创建，或者配置 `stage.url` 让工具自动创建。

#### Step 5: 配置 PITR

```
? Action: Edit PITR
? Select PITR: Create new PITR
? PITR name: pitr_tpcc_db_260302113046
? PITR range value: 7
? PITR range unit: d
```

PITR 是增量同步的基础。工具会列出已有的 PITR 供选择，也可以新建。

- **range value** — 保留时间长度（如 7）
- **range unit** — 时间单位：`h`(小时) / `d`(天) / `mo`(月) / `y`(年)

#### Step 6: 保存配置

```
? Action: Save
```

配置保存到 `config.json`，后续运行自动加载。

---

## 使用方式

### 交互模式（默认）

直接运行进入 TUI 交互菜单：

```bash
python branch_cdc.py
```

主菜单选项：

| 选项 | 说明 |
|------|------|
| **Manual Sync Now** | 手动触发一次同步，可选 Incremental（增量）或 Full（全量） |
| **Verify Consistency** | 手动校验上下游数据一致性，可选择具体表和快照时间点 |
| **Automatic Mode** | 进入自动循环同步，需设置间隔秒数 |
| **Edit Configuration** | 重新进入配置向导修改配置 |
| **Exit** | 退出程序 |

### 命令行参数

支持通过命令行参数跳过交互菜单，直接执行同步：

```bash
# 单次同步后退出
python branch_cdc.py --once

# 自动模式，每 30 秒同步一次
python branch_cdc.py --mode auto --interval 30

# 自动模式，每 10 次同步做一次一致性校验
python branch_cdc.py --mode auto --interval 30 --verify-interval 10

# 指定配置文件路径
python branch_cdc.py --config /path/to/my_config.json

# 指定日志文件路径
python branch_cdc.py --log-file /path/to/sync.log
```

完整参数列表：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--config` | string | `./config.json` | 配置文件路径 |
| `--log-file` | string | `./cdc_sync.log` | 日志文件路径 |
| `--mode` | `manual`/`auto` | 无（进入交互菜单） | 运行模式 |
| `--interval` | int | config 中的 `sync_interval`（默认 60） | 自动模式同步间隔（秒） |
| `--verify-interval` | int | config 中的 `verify_interval` | 自动模式下每 N 次同步做一次一致性校验 |
| `--once` | flag | — | 执行一次同步后立即退出 |

---

## 配置文件详解（config.json）

工具的所有配置保存在 `config.json` 中，由配置向导自动生成，也可以手动编辑。

### 完整字段说明

```jsonc
{
    // --- 上游连接 ---
    "upstream": {
        "host": "127.0.0.1",       // MatrixOne 上游地址
        "port": "6001",             // 端口
        "user": "dump",             // 用户名
        "password": "111",          // 密码
        "db": "tpcc",               // 上游数据库名
        "table": "bmsql_stock"      // 上游表名（database 模式下可忽略）
    },

    // --- 下游连接 ---
    "downstream": {
        "host": "127.0.0.1",
        "port": "6001",
        "user": "dump",
        "password": "111",
        "db": "tpcc_ds",            // 下游数据库名（不存在会自动创建）
        "table": "bmsql_stock"      // 下游表名（不存在会自动创建）
    },

    // --- Stage 配置 ---
    "stage": {
        "name": "stage02",          // Stage 名称（必填）
        "url": ""                   // 可选，填写后工具会自动 CREATE STAGE
    },

    // --- PITR 配置 ---
    "pitr": {
        "name": "pitr_tpcc_db_260302113046",  // PITR 名称
        "level": "database",        // 级别：database 或 table（自动根据 sync_scope 决定）
        "obj_id": 376786,           // 目标对象 ID（自动获取）
        "length": 7,                // 保留时间长度
        "unit": "d"                 // 时间单位：h / d / mo / y
    },

    // --- 同步设置 ---
    "sync_scope": "database",       // 同步粒度：table 或 database
    "sync_interval": 60,            // 自动模式同步间隔（秒）
    "verify_interval": 50,          // 每 N 次同步做一次 FULL 一致性校验（0 = 不校验）
    "verify_columns": []            // FAST 校验使用的列（空 = 自动选择）
}
```

### 各字段用途详解

#### upstream / downstream

上下游的 MySQL 协议连接信息。工具通过 PyMySQL 分别连接上游（读取数据、执行 diff）和下游（写入数据、管理 watermark）。

- `db` — 上游是 diff 的源数据库；下游如果不存在，工具会自动 `CREATE DATABASE`
- `table` — 仅在 `sync_scope: "table"` 时有效。`database` 模式下上游库的所有表会被自动发现并逐表同步，`table` 字段被忽略

#### stage

`data branch diff` 命令会将差异数据输出到 Stage 存储路径。Stage 是 diff 文件的中转站，全量同步时下游通过 `LOAD DATA INFILE` 从 Stage 读取文件，增量同步时工具从 Stage 读取 SQL 文件并解析执行。

- `name` — 必填，对应 MatrixOne 中已创建的 Stage 名称，diff 命令通过 `stage://{name}` 引用
- `url` — 可选，如果填写，工具会在同步前自动执行 `CREATE STAGE IF NOT EXISTS`，省去手动建 Stage 的步骤

#### pitr

PITR 决定了上游能保留多长时间的历史快照。增量同步依赖 PITR：工具需要基于上次 watermark 时间点创建快照表，再与当前快照做 diff。如果 watermark 超出 PITR 保留范围，快照不可用，工具会自动回退全量同步。

- `name` — PITR 名称，每次同步前会校验该 PITR 是否存在且状态正常
- `level` — `database` 或 `table`，由 `sync_scope` 自动决定，不需要手动设置
- `obj_id` — 上游数据库或表的内部 ID，配置向导自动获取。用于校验 PITR 绑定的对象是否匹配（防止库/表被重建后 ID 变化导致 diff 错乱）
- `length` + `unit` — 保留时间范围，如 `7d` 表示保留 7 天快照。建议 ≥ 你的最大同步中断时间

#### sync_scope

决定同步粒度，影响整个工具的行为：

- `"table"` — 只同步 `upstream.table` → `downstream.table` 这一张表
- `"database"` — 自动发现上游库下所有表，逐表同步到下游库（表名保持一致）

切换 scope 后 PITR 配置会被清空（因为 PITR 的 level 和 obj_id 都变了），需要重新配置。

#### sync_interval

自动模式（`--mode auto` 或交互菜单的 "Automatic Mode"）下两次同步之间的等待秒数。命令行 `--interval` 参数会覆盖此值。

#### verify_interval

自动模式下每同步 N 次触发一次 FULL 一致性校验。设为 `0` 表示不做周期性校验。例如设为 `50`，则每 50 次同步后自动做一次全表哈希对比，确认上下游数据一致。

注意：除了这个周期校验，自动模式还会每 3 次增量同步对小表（< 1GB 且 < 10 万行）自动触发一次校验。

#### verify_columns

FAST 校验时使用的列名列表。默认为空，此时工具自动选择主键列做哈希对比；如果表没有主键，退化为 `COUNT(*)` 对比。手动指定列可以在不依赖主键的情况下提高校验覆盖度。

### 配置示例

#### Table 级同步

将上游 `tpcc.bmsql_stock` 同步到下游 `tpcc_ds.bmsql_stock`：

```json
{
    "upstream": {
        "host": "127.0.0.1",
        "port": "6001",
        "user": "dump",
        "password": "111",
        "db": "tpcc",
        "table": "bmsql_stock"
    },
    "downstream": {
        "host": "127.0.0.1",
        "port": "6001",
        "user": "dump",
        "password": "111",
        "db": "tpcc_ds",
        "table": "bmsql_stock"
    },
    "stage": { "name": "stage02" },
    "pitr": {
        "name": "pitr_tpcc_stock",
        "level": "table",
        "obj_id": 376800,
        "length": 7,
        "unit": "d"
    },
    "sync_scope": "table",
    "sync_interval": 60,
    "verify_interval": 50,
    "verify_columns": []
}
```

#### Database 级同步

将上游 `tpcc` 整库同步到下游 `tpcc_ds`（所有表自动同步）：

```json
{
    "upstream": {
        "host": "127.0.0.1",
        "port": "6001",
        "user": "dump",
        "password": "111",
        "db": "tpcc",
        "table": "bmsql_stock"
    },
    "downstream": {
        "host": "127.0.0.1",
        "port": "6001",
        "user": "dump",
        "password": "111",
        "db": "tpcc_ds",
        "table": "bmsql_stock"
    },
    "stage": { "name": "stage02" },
    "pitr": {
        "name": "pitr_tpcc_db",
        "level": "database",
        "obj_id": 376786,
        "length": 7,
        "unit": "d"
    },
    "sync_scope": "database",
    "sync_interval": 60,
    "verify_interval": 50,
    "verify_columns": []
}
```

> database 模式下 `table` 字段会被忽略，上游库下所有表自动发现并同步。

---

## 同步机制

### 工作原理

工具利用 MatrixOne 的 `data branch diff` 能力，对比两个时间点的数据快照，生成差异 SQL 文件，然后将差异应用到下游数据库。

核心流程：

```
上游数据库 → PITR 快照 → data branch diff → Stage 文件 → 应用到下游
```

### FULL（全量同步）

在以下情况自动触发全量同步：

- 首次同步（无 watermark 记录）
- 手动选择 "Full Sync"
- 上一次的 watermark 对应的快照已过期（超出 PITR 保留范围）
- watermark 一致性校验失败

全量同步流程：
1. 在上游创建一个空表 `{table}_zero` 和当前快照表 `{table}_copy_now`
2. 对两者执行 `data branch diff`，生成包含全部数据的文件
3. 下游执行 `TRUNCATE` 清空目标表
4. 通过 `LOAD DATA INFILE` 将 diff 文件导入下游
5. 记录新的 watermark（MO_TS 纳秒时间戳）

### INCREMENTAL（增量同步）

当存在有效的 watermark 时自动进入增量模式：

1. 在上游基于上次 watermark 创建快照表 `{table}_copy_prev`
2. 基于当前时间创建快照表 `{table}_copy_now`
3. 对两者执行 `data branch diff`，生成增量 SQL（INSERT/DELETE/UPDATE）
4. 解析 diff SQL，将上游库表名重写为下游库表名
5. 逐条应用到下游
6. 更新 watermark

如果增量 diff 为空（NOOP），跳过应用，不更新 watermark。

### Watermark 管理

- watermark 存储在下游的 `branch_cdc_db.meta` 表中
- 每个同步任务由 `task_id` 唯一标识（基于上下游连接信息生成）
- 系统保留最近 4 个 watermark，自动清理旧记录
- watermark 值为纳秒级时间戳（MO_TS）

---

## 一致性校验

工具内置两种一致性校验模式，用于验证上下游数据是否一致。

### FAST 校验

- 仅选择主键列或 `verify_columns` 指定的列子集进行哈希对比，速度快
- 使用 `BIT_XOR(CRC32(CONCAT_WS(...)))` 对选定列计算聚合哈希
- 如果没有主键且未配置 `verify_columns`，退化为 `COUNT(*)` 对比
- 可通过 `verify_columns` 配置指定校验列

### FULL 校验

- 选择全部列进行哈希对比，精度最高
- 使用 `BIT_XOR(CRC32(CONCAT_WS(...)))` 对全表所有列计算聚合哈希
- 适合数据量较小的表，或定期做终极一致性确认
- 大表（超过 1GB 或 10 万行）在自动模式下会跳过 FULL 校验

### 校验触发方式

| 方式 | 说明 |
|------|------|
| 手动校验 | 交互菜单选择 "Verify Consistency"，可选表和快照时间点 |
| 自动周期校验 | 配置 `verify_interval`，每 N 次同步自动触发一次 FULL 校验 |
| 增量周期校验 | 自动模式下每 3 次增量同步自动触发一次校验（小表） |
| watermark 校验 | 每次增量同步前自动校验 watermark 一致性，失败则回退全量 |

---

## 前置条件

### MatrixOne 要求

- 上游 MatrixOne 需支持 `data branch diff` 语法
- 需要预先创建 Stage（或在 config 中配置 `stage.url` 让工具自动创建）
- 上游用户需要有创建/删除表、创建 PITR、执行 data branch 操作的权限
- 下游用户需要有创建数据库/表、INSERT/DELETE/UPDATE/TRUNCATE 权限

### Stage 准备

如果 MatrixOne 中还没有 Stage，需要先手动创建：

```sql
CREATE STAGE stage02 URL='file:///tmp/stage02';
```

或者在 config.json 中填写 `stage.url`，工具会自动执行 `CREATE STAGE IF NOT EXISTS`。

### PITR 说明

PITR（Point-In-Time Recovery）是增量同步的基础。它决定了上游能保留多长时间的历史快照：

- 如果 PITR 保留 7 天，那么 7 天内的任意时间点都可以做 diff
- 如果上次同步的 watermark 超出了 PITR 保留范围，工具会自动回退到全量同步
- 建议 PITR 保留时间 ≥ 你的最大同步中断时间

---

## 分布式锁机制

工具使用基于数据库的分布式锁防止多实例并发同步同一任务：

- 锁存储在下游 `branch_cdc_db.meta_lock` 表中
- 每个任务由 `task_id`（基于上下游连接信息生成）唯一标识
- 锁超时时间为 30 秒，由 `LockKeeper` 守护线程每 10 秒自动续约
- 如果同步过程中锁丢失，当前同步周期会自动跳过

---

## 日志与排错

### 日志文件

- 默认路径：`./cdc_sync.log`（与脚本同目录）
- 可通过 `--log-file` 参数自定义路径
- 同时输出到终端（带 Rich 格式化）和日志文件

### 日志格式说明

每次同步完成后会输出一行汇总日志：

```
Sync duration=0.523/0.187/1.234s status=SUCCESS
```

三个时间分别是：diff 耗时 / apply 耗时 / 总耗时。

### 常见问题

| 问题 | 原因 | 解决方式 |
|------|------|----------|
| `PITR not configured` | config.json 中缺少 pitr 配置 | 运行交互模式，选择 Edit PITR 完成配置 |
| `PITR xxx not found` | PITR 已被删除或名称不匹配 | 重新创建 PITR 或修改 config |
| `PITR xxx obj_id mismatch` | 数据库/表被重建导致 obj_id 变化 | 重新配置 PITR（Edit PITR） |
| `MO_TS xxx unavailable` | watermark 超出 PITR 保留范围 | 工具会自动回退全量同步，无需干预 |
| `Stage name is missing` | config.json 中缺少 stage.name | 配置 stage 名称 |
| `Lock lost before sync` | 另一个实例抢占了锁 | 确保同一任务只有一个实例运行 |
| `Watermark inconsistent` | 上下游数据不一致 | 工具会自动回退全量同步修复 |
| 连接失败 | 上游或下游 MatrixOne 不可达 | 检查 host/port/user/password 配置 |

---

## 典型使用场景

### 场景 1：单表实时同步

将生产库的一张热表同步到分析库，每 30 秒增量同步：

```bash
python branch_cdc.py --mode auto --interval 30
```

config.json 中设置 `sync_scope: "table"`，指定具体的上下游表名。

### 场景 2：整库定期同步

将整个数据库同步到备份库，每 60 秒一次，每 50 次做一次全量校验：

```bash
python branch_cdc.py --mode auto --interval 60 --verify-interval 50
```

config.json 中设置 `sync_scope: "database"`。

### 场景 3：一次性全量迁移

只做一次全量同步后退出，适合初始化场景：

```bash
python branch_cdc.py --once
```

首次运行无 watermark，自动执行全量同步。

### 场景 4：定时任务集成

配合 cron 使用，每 5 分钟触发一次增量同步：

```cron
*/5 * * * * cd /path/to/branch_cdc && python branch_cdc.py --once >> /var/log/cdc.log 2>&1
```
