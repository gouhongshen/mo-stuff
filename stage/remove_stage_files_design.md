# MatrixOne Stage Remove Files Design

## 背景
MatrixOne 已支持 stage:// 作为外部存储的逻辑入口，但缺少“删除 stage 内文件”的 SQL 语法。用户需要在 SQL 层直接清理 stage 文件，尤其在 `SELECT INTO OUTFILE`、`save_file` 产生临时文件时。

本设计新增 SQL 语法 `REMOVE FILES FROM STAGE`，提供单文件和通配删除能力，并支持 `IF EXISTS` 的安全模式。

## 目标
- 提供标准 SQL 语法删除 stage 文件。
- 支持通配符删除 ("*" / "?")。
- 提供 `IF EXISTS`，允许无匹配时不报错。
- 可用于 file/s3/hdfs 等 stage 后端。
- 与现有 stage 体系、权限体系兼容。

## 非目标
- 不新增 `delete_file`/`delete_files` 函数（如需可独立讨论）。
- 不支持 non-stage:// URL 删除（仅 stage://）。
- 不提供目录级递归删除的显式语义（通过通配符实现即可）。

## 为什么不采用 `select delete_file(cast('...' as datalink))`
1. **语义冲突**：`datalink` 设计是单文件引用（还支持 offset/size），删除操作天然是“全文件级”，与 datalink 的片段语义不一致。\n2. **多文件不自然**：如果要支持通配或批量删除，`datalink` 需要扩展为列表或通配解析，语义会变得混乱且难以维护。\n3. **语法可读性差**：`select delete_file(cast('stage://...' as datalink))` 比 `REMOVE FILES FROM STAGE 'stage://...'` 更绕，且更像“函数副作用”而不是 DDL/DML 风格语句。\n4. **与已有 stage 体系契合度低**：已有 `create/alter/drop stage` 与 `stage_list` 都是语句/TVF 形态，删除更适合作为明确的 stage 操作语句。

## SQL 语法
```
REMOVE FILES FROM STAGE 'stage://<stage>/<path_or_pattern>'
REMOVE FILES FROM STAGE IF EXISTS 'stage://<stage>/<path_or_pattern>'
```

- `<path_or_pattern>` 支持 `*` 与 `?` 通配。
- 仅允许 `stage://`；其它协议将报错。

## 行为定义
1. **单文件**：精确删除指定文件。
2. **通配符**：列出匹配文件后逐个删除。
3. **IF EXISTS**：当无匹配文件时不报错，返回成功。
4. **无 IF EXISTS**：无匹配时返回 “file ... is not found”。
5. **带 query 参数**：拒绝执行（例如 `?offset=...`），返回错误。

## 错误与兼容性
- 错误消息保持与现有文件找不到的语义一致：`file ... is not found`。
- 解析输出格式统一为小写 SQL。
- 仅新增语法，不影响现有语句。

## 权限模型
沿用现有 stage 语句的权限处理（`Create/Alter/Drop Stage` 同等级）。

## 实现方案

### 1) Parser
- 关键字新增：`REMOVE`, `FILES`。
- 语法规则新增：`remove_stage_files_stmt`。
- 生成 `tree.RemoveStageFiles` AST。

相关文件：
- `pkg/sql/parsers/dialect/mysql/mysql_sql.y`
- `pkg/sql/parsers/dialect/mysql/keywords.go`
- `pkg/sql/parsers/dialect/mysql/mysql_sql.go`（由 goyacc 生成）

### 2) AST
新增节点：
- `tree.RemoveStageFiles`
- 包含字段：`IfExists`、`Path`
- 语句类型：`frontendStatusTyp`

相关文件：
- `pkg/sql/parsers/tree/stages.go`
- `pkg/sql/parsers/tree/stmt.go`

### 3) Frontend 执行路径
- 在 `self_handle.go` 增加 dispatch。
- 在 `mysql_cmd_executor.go` 增加 handler。
- 在 `authenticate.go` 增加 `doRemoveStageFiles` 并接入权限逻辑。

相关文件：
- `pkg/frontend/self_handle.go`
- `pkg/frontend/mysql_cmd_executor.go`
- `pkg/frontend/authenticate.go`
- `pkg/frontend/types.go`

### 4) 删除核心实现
新增 `stageutil.DeleteStageFiles`：
- 输入：`ctx`, `proc`, `stagePath`, `ifExists`
- 使用 `UrlToStageDef` 展开 stage
- 拒绝 query 参数
- 将路径映射为 fileservice 目标
- 若含通配符：走 `StageListWithPattern`
- 删除前 `StatFile` 保障 IF EXISTS 语义

注意点：
- 使用**请求 ctx**，避免语句执行阶段出现 `context canceled`。

相关文件：
- `pkg/stage/stageutil/stageutil.go`

## 数据流
```
SQL -> Parser -> AST RemoveStageFiles -> Frontend handler
 -> stageutil.DeleteStageFiles(ctx, proc, path, ifExists)
 -> UrlToStageDef -> ToPath -> (StageListWithPattern?) -> ETL FileService Delete
```

## 测试方案

### 单测
- `pkg/stage/stageutil` 新增 `TestDeleteStageFiles`：
  - 单文件删除
  - 通配删除
  - IF EXISTS 无匹配
  - 无 IF EXISTS 无匹配错误

### BVT
- 新增用例：`test/distributed/cases/stage/remove_stage_files.sql`
- 覆盖：
  - 单文件删除
  - 通配删除
  - IF EXISTS 无匹配
  - 无 IF EXISTS 报错
  - 清理残留文件
- 结果文件：`test/distributed/cases/stage/remove_stage_files.result`

### 覆盖率
- 包级覆盖率（`pkg/stage/stageutil`）：约 53.9%
- 新增函数 `DeleteStageFiles` 约 74.2%

### 手动验证建议
1. `save_file` 写入 stage
2. `REMOVE FILES FROM STAGE` 删除
3. `load_file` 确认不存在

## 风险与回滚
- 风险：通配范围过大导致误删，建议默认使用精确路径，生产可结合 `IF EXISTS` 与小范围通配。
- 回滚：移除语法与 stageutil 删除逻辑即可恢复原状。

## 变更清单（关键文件）
- Parser/AST: `mysql_sql.y`, `mysql_sql.go`, `keywords.go`, `stages.go`, `stmt.go`
- Frontend: `self_handle.go`, `mysql_cmd_executor.go`, `authenticate.go`, `types.go`
- Core: `stageutil.go`
- Tests: `stageutil_test.go`, `remove_stage_files.sql/.result`
