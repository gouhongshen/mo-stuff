# MatrixOne Data Branch CDC 技术架构与验证报告 (v3.1)

> **Note**: 本版本针对第三方 Review 意见进行了深度加固，解决了锁竞争、数据重入及校验深度等核心问题。

## 1. 方案概述 (Overview)
`branch_cdc.py` 是一个基于 MatrixOne **Data Branch** 特性实现的增量同步工具。它通过快照（Snapshot）和分支比对（Diff）技术，实现了从上游（Upstream）到下游（Downstream）的逻辑数据复制。

---

## 2. 核心状态转移模型 (Core State Transitions)

同步进程严格遵循以下流水线状态机，确保在分布式环境下的原子性与自愈能力：

### 2.1 初始化与锁机制 (Init & Locking)
*   **分布式锁 (Multi-instance Safety)**: 执行 `acquire_lock()`。
    *   **抢占逻辑**: `(lock_owner IS NULL) OR (owner=self) OR (lock_time < NOW() - 30s)`。
    *   **锁续约 (Heartbeat)**: 在耗时的 `diff` 捕获后及数据应用前，调用 `heartbeat_lock()` 刷新锁时间，防止长同步被误抢占。
*   **DB 自动创建**: 自动检查并创建下游数据库及 `meta` 管理库。

### 2.2 考古恢复模式 (Archeology Recovery)
*   **触发条件**: 当 `meta` 记录丢失且下游已有数据时自动启动。
*   **恢复路径**:
    1.  扫描上游所有属于该 `task_id` 的历史快照。
    2.  利用 **1% 抽样哈希校验** 进行反向探测。
    3.  一旦匹配，立即恢复 `meta` 水位线，避免重复执行 `FULL` 同步。
*   **防重底座**: 如果考古模式无法找回水位，必须执行 `FULL` 同步时，会先执行 `TRUNCATE TABLE` 强制清理下游，确保数据零重复。

### 2.3 差异捕获与事务应用 (Atomic Apply)
*   **差异生成**: 利用 `data branch diff ... output file` 生成变更集。
*   **事务控制**: 
    *   **严格事务模型**: 显式关闭驱动层 `autocommit`，确保 Session 始终受 Python 事务控制。在任何 DDL (如 TRUNCATE) 后强制执行 `COMMIT` 以清空隐式事务上下文。
    *   **事务剥离**: 使用更健壮的正则表达式从 SQL 文件中滤除 `BEGIN/COMMIT/ROLLBACK/START TRANSACTION`。
    *   **SQL 重写**: 动态解析 `INSERT/UPDATE/REPLACE/DELETE` 语句，确保 schema 与下游目标一致。
*   **原子提交**: 数据变更 DML 与 `meta` 水位线更新在同一个物理事务中 `COMMIT`。

---

## 3. 分级验证模型 (Tiered Verification)

系统内置了两种维度的校验，在 `sync_loop` 中自动切换：

| 校验级别 | 触发频率 | 技术细节 | 目的 |
| :--- | :--- | :--- | :--- |
| **FAST Check** | 每 5 次同步 | **1% 抽样哈希**: 基于主键执行 `ABS(HASHTYPE(pk)) % 100 = 7` | 探测行内数据篡改 |
| **FULL Check** | 每 N 次同步 | 全表审计: `BIT_XOR(CRC32(CONCAT_WS(...)))` | 终极一致性保证 |

---

## 4. 自动化验证体系 (Review Verified)

针对 Review 意见，我们强化了以下测试维度：

| 测试工具 | 修复验证点 | 动作描述 |
| :--- | :--- | :--- |
| `test_suite.py` | **考古模式验证** | 模拟 `DROP DATABASE meta`，验证是否产生重复数据。 |
| `test_suite.py` | **抽样快检验证** | 模拟 1/1000 概率的数据篡改，验证 `Fast Check` 的灵敏度。 |
| `stress_test.py` | **锁竞争验证** | 移除手动清锁，验证 30s TTL 自动回收与续约逻辑。 |
| `db_crash_test.py` | **可移植性修复** | 移除硬编码路径，支持在任意 MO 源码环境下运行。 |

---

## 5. 资源治理 (GC)
*   **快照管理**: 上游始终只保留最近 4 个快照，自动执行 `DROP SNAPSHOT`。
*   **文件清理**: 使用 `REMOVE` 命令物理清理 Stage 存储，通过 `try-except` 保证主流程不受清理异常干扰。