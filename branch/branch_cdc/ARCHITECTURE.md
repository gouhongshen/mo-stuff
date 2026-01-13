# MatrixOne Data Branch CDC 技术架构与验证报告 (v4.0)

> **Note**: 本版本采用 **扁平化执行引擎**，彻底解决了分布式环境下快照丢失导致的递归异常，并针对 MatrixOne 快照全局属性加强了考古匹配精度。

## 1. 方案概述 (Overview)
`branch_cdc.py` 是一个基于 MatrixOne **Data Branch** 特性实现的工业级增量同步工具。

---

## 2. 核心状态转移模型 (Core State Transitions)

### 2.1 基础设施初始化 (Infrastructure First)
*   **连接解耦**: 采用“ foundation session” 模式，即使目标数据库物理上不存在，脚本依然能成功建立基础连接并自动执行 `CREATE DATABASE`。
*   **分布式锁 (LockKeeper Thread)**: 引入独立的守护线程，每 10s 自动续约锁有效期，彻底解决长同步任务（如大表 Diff 或大规模 DML 应用）中的锁抢占风险。

### 2.2 考古恢复与防重 (Archeology & Deduplication)
*   **多维考古匹配**: `archeology_recovery` 不仅比对数据哈希，还强制验证快照的 `database_name` 和 `table_name` 属性，杜绝了多任务环境下的假阳性匹配。
*   **回退安全性**: 
    *   在 `FULL Sync` 路径中，强制遵循“先 Diff、后 Truncate”的原子顺序，确保同步前置失败时下游数据零损失。
    *   在 `INCREMENTAL` 路径中，若检测到上游快照失效，系统会自动触发 `Watermark Reset` 并回退至全量同步模式。

### 2.3 扁平化执行引擎 (Linear Execution)
*   **无递归设计**: 所有的重试与模式切换均通过显式的逻辑跳转实现，消除了 Python 栈溢出的隐患。
*   **SQL 重写审计**: 增强版正则重写器支持 SQL Hint、多表 JOIN 变体及 `INSERT/UPDATE` 的重定向。

---

## 3. 分级验证模型 (Tiered Verification)

| 校验级别 | 触发频率 | 技术细节 | 目的 |
| :--- | :--- | :--- | :--- |
| **FAST Check** | 每 5 次同步 | **1% 抽样哈希**: 利用 `__mo_fake_pk_col` 或 PRI 采样 | 探测行内数据篡改 |
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