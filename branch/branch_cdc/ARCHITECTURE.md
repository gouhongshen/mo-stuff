# MatrixOne Data Branch CDC 技术架构与验证报告 (v3.0)

## 1. 方案概述 (Overview)
`branch_cdc.py` 是一个基于 MatrixOne **Data Branch** 特性实现的增量同步工具。它通过快照（Snapshot）和分支比对（Diff）技术，实现了从上游（Upstream）到下游（Downstream）的逻辑数据复制。

---

## 2. 详细状态转移模型 (Detailed State Transitions)

同步进程严格遵循以下流水线状态机，任何一个步骤失败都会触发相应的回滚或熔断逻辑：

### 2.1 状态 1: 初始化与预检 (Initialization)
*   **Step 1.1**: 加载 `config.json`。若缺失且处于非交互模式，进程立即终止。
*   **Step 1.2**: 建立双端连接 (`Up`, `Ds`)。使用 `autocommit=True` 建立基础会话。
*   **Step 1.3**: 执行 `ensure_meta_table()`。在下游创建 `cdc_by_data_branch_db.meta`。
*   **Step 1.4 (分布式锁)**: 执行 `acquire_lock()`。
    *   尝试更新 `lock_owner`。
    *   判断逻辑：`(lock_owner IS NULL) OR (owner=self) OR (lock_time < NOW() - 30s)`。
    *   若获取失败，记录 `locked. Skipping` 并结束当前周期。

### 2.2 状态 2: 目标对象准备 (Object Materialization)
*   **Step 2.1**: 检查上游库表是否存在。
*   **Step 2.2**: 检查下游库是否存在。
*   **Step 2.3**: 探测下游表。若表不存在：
    1.  从上游抓取 `SHOW CREATE TABLE`。
    2.  在 Python 层替换表名为下游目标表名。
    3.  在下游执行 DDL 重建表结构。

### 2.3 状态 3: 水位线探测与自愈 (Watermark & Recovery)
*   **Step 3.1**: 从 `meta` 表中获取 `last_good_watermark`。
*   **Step 3.2 (考古模式/自愈)**: 若 `meta` 为空但下游表有数据：
    1.  扫描上游所有 `cdc_{short_id}_%` 格式的快照。
    2.  按时间倒序执行 `verify_consistency` 哈希比对。
    3.  一旦匹配成功，立即将该快照存入 `meta` 作为断点续传的基准。
*   **Step 3.3**: 确定模式：若存在基准快照则进入 `INCREMENTAL`，否则进入 `FULL`。

### 2.4 状态 4: 差异捕获 (Capture Change Set)
*   **Step 4.1**: 生成 `new_snap` 名称（MD5 哈希前缀 + 微秒时间戳）。
*   **Step 4.2 (FULL)**: 
    1.  在上游创建临时的 `_zero` 空表。
    2.  执行 `data branch diff ... newsnap against _zero output file 'stage://...'`。
*   **Step 4.3 (INCREMENTAL)**:
    1.  在上游从 `last_good_watermark` 创建临时表 `_copy_prev`。
    2.  创建 `new_snap` 快照。
    3.  执行 `data branch diff ... newsnap against _copy_prev output file 'stage://...'`。

### 2.5 状态 5: 原子事务应用 (Atomic Apply)
*   **Step 5.1**: 下游开启事务 `ds_conn.execute("BEGIN")`。
*   **Step 5.2 (Data Load)**:
    *   **FULL 模式**：构造 `LOAD DATA INFILE` 语句。强制指定 `ESCAPED BY '\\'`（双反斜杠）。
    *   **INCREMENTAL 模式**：
        1.  使用 `select load_file(...)` 将差异 SQL 内容读入 Python 内存。
        2.  **事务剥离**：利用正则 `re.sub` 移除文件自带的 `BEGIN;` 和 `COMMIT;`。
        3.  **表名重定向**：将 SQL 中的原始表名替换为下游实际表名。
        4.  **逐条执行**：在当前事务会话中顺序执行变更语句。
*   **Step 5.3 (Meta Update)**: 将 `new_snap` 写入 `meta` 表。
*   **Step 5.4**: 执行 `COMMIT`。若失败则 `ROLLBACK`。

### 2.6 状态 6: 资源治理 (Resource GC)
*   **Step 6.1**: 清理上游 `_zero` 或 `_copy_prev` 临时表。
*   **Step 6.2**: 调用 `REMOVE` 命令删除外部 Stage 里的差异 CSV/SQL 文件。
*   **Step 6.3**: 检查上游快照数量。若超过 4 个，循环执行 `DROP SNAPSHOT`。
*   **Step 6.4**: 执行 `release_lock()` 释放分布式锁。

---

## 3. 分级验证模型 (Tiered Verification)

在 `sync_loop` 自动化运行期间，系统会根据同步次数（sc）自动跳转到验证状态：

| 同步计数 (sc) | 触发操作 | 校验深度 | 性能开销 |
| :--- | :--- | :--- | :--- |
| `sc % 5 == 0` | **FAST Verify** | 对比 `COUNT(*)` 行数 | 毫秒级 |
| `sc % N == 0` | **FULL Verify** | 全表 `BIT_XOR` 哈希审计 | 分钟级 (取决于 IO) |

---

## 4. 自动化测试矩阵 (Test Report)

| 测试工具 | 验证场景 | 核心动作 | 最终状态 |
| :--- | :--- | :--- | :--- |
| `test_suite.py` | 多下游一致性 | 并发同步，篡改下游数据 | **CATCHED** (捕获篡改) |
| `test_suite.py` | 灾难恢复 | `DROP DATABASE meta` | **HEALED** (自动重建) |
| `stress_test.py` | 脚本稳定性 | 持续 `kill -9` 同步脚本 | **RESUMED** (断点续传) |
| `db_crash_test.py` | 数据库可靠性 | 强制关闭 `mo-service` | **CONSISTENT** (哈希匹配) |