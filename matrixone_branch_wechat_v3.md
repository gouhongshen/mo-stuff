# 在数据库里玩“平行宇宙”：MatrixOne Data Branch 让数据也拥有 Git 的分支/合并/对比/回滚（含跨集群同步）

> 这篇文章讲一件事：**当数据像代码一样频繁迭代时，数据库内生的“分支工作流”会比备份/复制更可靠、更省钱、更工程化。**

---

## 1. 从一个真实的“数据事故现场”开始

你一定见过这种场景：

- 业务突然说：“昨天的报表不对。”
- 研发追问：“到底哪里不对？”
- 数据同学回答：“我昨晚跑了个脚本……可能把某几行更新了，也可能删了……我不太确定。”

然后团队进入经典三连：  
1) **猜**是哪一步改坏了；  
2) **拷贝一份数据**再试（TB 级复制，心在滴血）；  
3) **一边修一边祈祷**别再影响线上。

本质原因是：**数据研发还停留在“前 Git 时代”**——缺少隔离的实验环境、缺少可追溯的变更历史、缺少可控的合并与回滚。

---

## 2. 为什么“数据”比“代码”更需要 Branch？

代码版本控制之所以成功，是因为它解决了三个核心问题：

1. **并行**：每个人都有自己的分支，互不干扰  
2. **可追溯**：任何变更都能定位“谁在何时做了什么”  
3. **可回滚**：出事了，回到一个确定的健康版本

数据同样需要这三件事，但难度更高：

- **体量更大**：代码几 MB，数据可能是 TB / PB  
- **副作用更强**：一次 UPDATE/DELETE 可能把“全公司共用”的基准表改坏  
- **链路更长**：错误会沿着下游任务扩散  
- **合规更严**：要解释来源与变更路径（可审计）

因此，“数据分支”不能只是“复制一份表”。它必须做到：

- **低成本克隆**（尽量共享底层存储）  
- **隔离写入**（你改你的，不影响主干）  
- **可对比**（差异能落到行/列级别）  
- **可合并**（把实验成果安全送回主干）  
- **可恢复**（回到任意 snapshot 的状态）

---

## 3. 行业里大家怎么做“数据分支”？

“数据分支”不是 MatrixOne 一家在想的事。不同体系，做法不同：

- **湖仓/表格式（Iceberg）**：Branch/Tag 是“指向 snapshot 的命名引用”，擅长管理快照谱系与发布流程  
- **DataOps（lakeFS）**：把对象存储的数据当作仓库做 branch/commit/merge  
- **云数仓（Snowflake/Databricks）**：用 zero-copy clone / table clone 快速派生环境  
- **数据库侧（Neon/PlanetScale/Dolt）**：把“分支”融入数据库开发/CI 或把数据库做成“Git for Data”

共同趋势很清晰：**分支正在从软件工程的基础设施，变成数据工程的基础设施。**

---

## 4. MatrixOne Data Branch：把版本控制做成 SQL 能力

MatrixOne 的 Data Branch 不是“又一个备份功能”，而是一套数据库内的工作流：

- **Snapshot（快照）**：给当前状态打“存档点”  
- **Branch（分支表）**：从某个快照拉出隔离工作区（可做 DML）  
- **Diff（对比）**：任意两个版本/分支能做差异对比  
- **Merge（合并）**：把分支改动合并回主干（支持冲突策略）  
- **Restore（回滚）**：不满意？直接回到某个快照

你可以把它理解为：

> 在数据库里，**Snapshot ≈ commit 点**，**Branch ≈ 分支引用**，**Diff/Merge/Restore ≈ Git 操作**，而且一切都是 SQL。

---

## 5. 一套完整可跑的 SQL：base → snapshot → branch → dml → diff/merge → restore

### 5.1 初始化：base 表 + 初始数据

```sql
drop database if exists demo_branch;
create database demo_branch;
use demo_branch;

create table orders_base (
  order_id     bigint primary key,
  user_id      bigint,
  amount       decimal(12,2),
  risk_flag    tinyint,           -- 0=正常, 1=高风险
  promo_tag    varchar(20),       -- 活动标记
  updated_at   timestamp
);

insert into orders_base values
(10001, 501,  99.90, 0, null, '2025-12-01 10:00:00'),
(10002, 502, 199.00, 0, null, '2025-12-01 10:00:00'),
(10003, 503,  10.00, 0, null, '2025-12-01 10:00:00');
```

### 5.2 第一次“存档”：给 base 打快照（sp_v1）

```sql
create snapshot sp_orders_v1 for table demo_branch orders_base;
show snapshots;
```

### 5.3 从快照拉两条分支：风险修复线 & 活动实验线

```sql
data branch create table orders_riskfix from orders_base{snapshot='sp_orders_v1'};
data branch create table orders_promo  from orders_base{snapshot='sp_orders_v1'};
```

### 5.4 在分支上随便改：互不干扰

```sql
-- 风险修复分支：标记高风险、删除并修复一条记录
update orders_riskfix
   set risk_flag = 1,
       updated_at = '2025-12-01 10:05:00'
 where order_id = 10002;

delete from orders_riskfix where order_id = 10003;

insert into orders_riskfix values
(10003, 503, 10.00, 0, 'repaired', '2025-12-01 10:06:00');


-- 促销分支：打活动标记 + 打折 + 新增订单
update orders_promo
   set promo_tag = 'double11',
       amount = amount * 0.9,
       updated_at = '2025-12-01 10:07:00'
 where order_id in (10001, 10002);

insert into orders_promo values
(10004, 504, 39.90, 0, 'double11', '2025-12-01 10:07:30');
```

### 5.5 给分支头再打快照：为了可重复 diff

```sql
create snapshot sp_riskfix for table demo_branch orders_riskfix;
create snapshot sp_promo   for table demo_branch orders_promo;
```

### 5.6 Diff：两条分支到底改了什么？

```sql
data branch diff orders_riskfix{snapshot='sp_riskfix'}
  against orders_promo{snapshot='sp_promo'};
```

### 5.7 Merge：把“正确的改动”合并回主干

```sql
-- 先把风险修复合回主干
data branch merge orders_riskfix into orders_base;

-- 再尝试把促销合回主干（冲突策略示例）
data branch merge orders_promo into orders_base when conflict skip;
-- data branch merge orders_promo into orders_base when conflict accept;
```

### 5.8 不满意？Restore：把主干一键回到 sp_orders_v1

```sql
restore database demo_branch table orders_base from snapshot sp_orders_v1;
```

---

## 6. 一张图看懂：数据血缘 + diff/merge/restore（Mermaid 兼容版）
![mermaid diagram](./pics/mermaid.svg)

---

## 7. 进阶玩法：Branch 也是“数据同步/跨集群同步”的基础设施

很多人第一次看到 Data Branch，会把它理解成“数据研发协作工具”。  
但它还有一个更工程化的隐藏技能：**把 diff 变成可携带、可重放的“数据补丁（patch）”**。

一句话：  
> **在源集群算出 diff → 把 diff 输出成文件（本地或 stage）→ 在任意地方重放（apply）这份 diff。**

这对“跨集群同步 / 预发环境刷新 / 灾备演练 / 分布式团队协作”特别有用。

### 7.1 Diff 输出成文件：CSV（全量初始化）或 SQL（增量补丁）

MatrixOne 支持把 `DATA BRANCH DIFF ...` 的结果直接输出成文件：`OUTPUT FILE '<dir>'`。

```sql
data branch diff branch_tbl against base_tbl output file '/tmp/diffs/';
```

官方用例里给了两种输出形态（非常关键）：

1) **目标为空（初始化同步）→ 输出 CSV**，并提示正确的 FIELDS/LINES 格式  
2) **目标非空（增量同步）→ 输出 .sql 补丁**，Hint 明确告诉你会包含 `DELETE FROM` + `REPLACE INTO`

> 直觉理解：  
> - 初始化：直接给你一份“全量可导入”的 CSV  
> - 增量：给你一份“可重放的 SQL patch”

### 7.2 跨集群同步：把 diff 写到 stage（例如 S3），另一端拉取并重放

你可以把 diff 输出到 stage（例如 S3）：

```sql
-- 源集群：创建一个外部 stage（S3 / 兼容 S3 的对象存储）
create stage stage01 url =
  's3://bucket/prefix?region=cn-north-1&access_key_id=xxx&secret_access_key=yyy';

-- 源集群：把 diff 直接写到 stage
data branch diff t1 against t2 output file 'stage://stage01/';
```

输出会告诉你保存到了哪个文件，并提示该 SQL 补丁主要由 `DELETE FROM ...`、`REPLACE INTO ...` 组成。

### 7.3 “Apply diff” 怎么做？（本质是重放生成的 SQL / 导入生成的 CSV）

这里有个非常务实的点：**MatrixOne 目前并不是再提供一条 `APPLY DIFF` 的新语句**，而是把 diff 产物变成“你能在任何地方执行/导入”的标准文件：

- 如果 diff 输出的是 **`.sql`**：在目标集群执行该 SQL 文件即可（相当于 apply patch）  
- 如果 diff 输出的是 **`.csv`**：在目标集群把 CSV 导入到目标表即可（相当于 bootstrap）

#### 7.3.1 重放 SQL diff（增量同步）

你可以用任意 MySQL 客户端方式执行 SQL 文件（因为 MatrixOne 兼容 MySQL 协议）。此外，MatrixOne 生态工具 `mo_ctl` 也支持直接执行 SQL 文件。

```bash
# 方式 A：用 mysql 客户端重放（示意）
mysql -h <mo_host> -P <mo_port> -u <user> -p <db_name> < diff_xxx.sql

# 方式 B：使用 mo_ctl 重放（示意）
mo_ctl sql diff_xxx.sql
```

#### 7.3.2 导入 CSV diff（初始化同步）

MatrixOne 支持 `LOAD DATA`，并且文件来源可以是本地文件或 `stage://...`。

```sql
load data local infile '/tmp/diffs/diff_xxx.csv'
into table demo_branch.orders_base
fields enclosed by '"' escaped by '\\' terminated by ','
lines terminated by '\n';
```

如果 CSV 在 stage 上，也可以用 `LOAD DATA INFILE 'stage://stage_name/path' ...` 的方式直接从对象存储导入。

---

## 8. 写在最后：Branch 不是“功能”，而是一种数据工程方法论

当你把 Data Branch 用在真实项目里，它会自然长成一套流程：

- **每次大规模清洗/标注/特征工程**：先打 snapshot，再开分支  
- **每个算法实验**：一人一分支（甚至每个 PR 一分支）  
- **上线前**：diff + review + merge（像 code review 一样 review 数据变更）  
- **跨集群/跨环境**：diff 输出成文件（本地或 stage）→ 在目标集群重放（apply）  
- **出事故**：restore 回健康快照，先止血，再排查

一句话总结：

> **Branch 不是“多一份副本”，而是“让数据协作与数据同步具备工程化的基本法”。**

---

## 参考链接（便于公众号放“阅读原文”）

```text
MatrixOne CREATE SNAPSHOT：
https://docs.matrixorigin.cn/v25.3.0.0/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/create-snapshot/

MatrixOne RESTORE SNAPSHOT：
https://docs.matrixorigin.cn/v25.3.0.0/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/restore-snapshot/

MatrixOne LOAD DATA：
https://docs.matrixorigin.cn/en/dev/MatrixOne/Reference/SQL-Reference/Data-Manipulation-Language/load-data-infile/
```
