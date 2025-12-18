# 在数据库里玩“平行宇宙”：MatrixOne Data Branch 让数据也拥有 Git 的分支/合并/对比/回滚（含跨集群同步）

> 这篇文章讲一件事：**当数据像代码一样频繁迭代时，数据库内生的“分支工作流”会比备份/复制更可靠、更省钱、更工程化。**

---

## 1. 从一个真实的“数据事故现场”开始

当你的 AI 模型不慎清空了核心数据库，或者错误地注入了大量虚假数据，传统的数据恢复手段往往让人崩溃——不仅耗时费力，还可能无法完全找回丢失的数据。然而，“Git for Data” 带来的全新范式能改变这一局面，让上述场景的恢复变得像回滚代码提交一样简单
。例如，在 MatrixOne 数据库中，只需一条命令就可以将数据库瞬间恢复到指定时间点，如：

```sql
Restore Database my_ai_data_db {snapshot/timestamp}
```

这样的能力源自于将 Git 的分支(version branch)理念引入数据管理，使我们能够像管理代码那样管理数据的版本和变更 。本文将结合当前数据管理的难点，介绍 MatrixOne 数据库最新推出的 Branch（分支）功能 是如何有效解决这些痛点的，并展示这一理念在行业内外的探索与应用。

---

## 2. 为什么数据也需要 Branch？：
### 数据管理的难点——版本混乱与协作困境

在进入正题之前，让我们先看看当今数据管理面临的典型难题。这些问题与十多年前软件开发在 Git 出现之前的境况十分类似：
* **数据变更如同“开盲盒”**：一次误操作或AI模型的“幻觉”导致的数据污染，往往难以追溯源头，更别提轻松回滚了。很多企业对于数据的修改缺乏完善的记录机制，一旦出现错误，需要耗费大量时间排查问题。
* **版本管理靠“复制粘贴”**：为了做实验或分析，数据工程师常常不得不复制出 TB 级别的数据副本。这样不仅成本高昂，而且产生了众多不同版本的孤立副本，版本混乱难以管理。不同人员可能各自为政，拷贝出自己的数据集，稍有不慎就会出现数据不一致。
* **团队协作靠“默契”**：多个团队并行处理同一批数据，经常互相干扰。由于缺乏工具支持，协同主要依赖人为约定，比如“不要动我那一份数据”，流程缺乏工程化保障 。这就像软件开发没有版本控制时，团队合作全凭信任，一不小心就冲突迭代，后果严重。

上述种种痛点表明，当前的数据管理仿佛仍停留在“Git 出现之前”的原始阶段 。正因如此，有人提出：如果数据是 AI 时代的“代码”，我们亟需一个面向数据的“Git”来彻底革新数据管理。这正是“数据分支”理念诞生的背景。

### 分支理念：让数据管理进入工程化新范式

“Git for Data”（数据版 Git）并非单指某个独立功能，而是全新的数据管理范式。它借鉴了软件工程领域成熟的版本控制思想，应用到数据全生命周期的管理中。本质上，这一范式围绕三大核心能力展开：
* **瞬间快照与快速回滚**：每一次数据变更都被记录留痕，可以随时为任何版本的数据打上“存档点”。一旦发生误操作或数据污染事件，我们可以一键回滚到上一个健康版本，整个过程在毫秒级或秒级内完成。传统“删库跑路”之所以可怕，是因为数据改动后难以复原；而有了可及时快照和回滚的能力，数据安全不仅靠权限控制，更有了随时恢复的底气
* **毫秒级克隆与隔离分支**：基于分支和零拷贝克隆技术，我们可以在毫秒内为每个数据工程师或每个实验创建独立且隔离的数据分支环境。这些分支共享底层存储，几乎不产生额外空间开销，因此克隆哪怕TB级的数据也不再耗时耗储存。团队成员能够各自在自己的数据分支上自由清洗、标注和测试模型，互不干扰；实验验证成功后，再将修改合并回主干。整个流程清晰、高效且安全，就如同多人在各自的代码分支上开发再合并代码一样。
* **版本差异对比与审计**：通过版本比较功能，我们可以清楚地看到两个数据版本（或两个分支）之间的所有差异，精确定位是哪一行、哪个字段的修改引发了问题。每一次数据变更的时间、操作者、修改内容都有据可查。这使得数据治理不再是黑盒过程，为数据质量控制和合规审计提供了强大支持换言之，谁在何时做了什么改动，一目了然，数据版本的“责任追溯”变得和代码版本一样透明。通过以上能力，数据管理终于可以像软件工程一样标准化、可追溯、可协作。数据分支让开发者能够安全地尝试大胆的假设，快速验证并回退错误。这极大提升了 AI 研发迭代速度——据实际经验，可将数据准备和模型验证周期从过去的数周缩短到数天。可以说，Git for Data 打破了 AI 开发的瓶颈，让数据不再成为拖累项目进度的障碍，而成为敏捷迭代的基石。


通过以上能力，**数据管理终于可以像软件工程一样标准化、可追溯、可协作**。数据分支让开发者能够安全地尝试大胆的假设，快速验证并回退错误。这极大提升了 AI 研发迭代速度——据实际经验，可将数据准备和模型验证周期从过去的数周缩短到数天。可以说，Git for Data 打破了 AI 开发的瓶颈，让数据不再成为拖累项目进度的障碍，而成为敏捷迭代的基石。

---

## 3. MatrixOne 的分支功能：数据库引擎中的“Git for Data”
要真正落地上述“Git 式”数据管理范式，离不开底层强大的数据引擎支持。MatrixOne 作为一款云原生超融合数据库，正是支撑这一新范式的有力底座。MatrixOne 是业界首个将 Git 风格的版本控制引入数据管理的数据库，在提供 MySQL 兼容性、AI 原生支持和云原生架构的同时，实现了类似 Git 的分支管理能力。 

MatrixOne 的分支功能涵盖了快照、分支、合并、时点恢复等完整的数据版本管理操作。例如，用户可以快速为当前数据库打快照并创建新的数据分支进行试验，所耗时间以毫秒计且几乎不占额外存储。多个分支共享存储的 Copy-on-Write 技术保证了零拷贝克隆，不论源数据集有多大，新分支都能瞬间产生。这意味着，即使是TB级的大数据表，MatrixOne 也可以在瞬间“复制”出一个供测试用的版本，而无需真正复制TB级的数据文件。 

更重要的是，MatrixOne 提供了方便的数据分支管理语法，支持像 Git 那样对分支进行创建、删除、切换和合并等操作。开发者能够直接在 SQL 界面上执行诸如“创建分支”“合并分支”的命令，来管理数据的不同版本分支，几乎和操作代码仓库没有区别。这种内置的版本控制功能还支持精细粒度的恢复和回滚——比如可以选择只回滚某张表的数据而不影响整库。从权限角度看，MatrixOne 也允许对不同分支设定不同访问权限，保障数据隔离和安全。

MatrixOne 分支功能背后的技术实现充分利用了现代数据库架构的优势。其存储计算分离和分布式架构，让分支的创建/删除开销极低，实例启动和扩容弹性敏捷。据官方介绍，MatrixOne 通过容器化和共享存储，实现了毫秒级创建新实例和分支的能力。这为 AI 时代频繁的数据迭代提供了前所未有的敏捷性和成本效益：开发者随时可以为新模型训练切出一条数据分支，试验成功后再安全地合并回主库，而整个过程对线上业务零扰动。 

值得一提的是，MatrixOne 并非只关注结构化数据的版本控制。作为多模态融合的数据平台，它能够对结构化表、半结构化JSON、甚至向量索引等各种数据类型统一进行快照和分支管理。这种底层统一性避免了以往不同系统各自为政导致的数据孤岛问题。因而，MatrixOne 的分支功能可以适用于企业数据的方方面面，从事务型业务数据到用于训练 AI 的大规模文本、图像数据，都可以纳入同一套版本管理框架下进行协作开发。 

综上，MatrixOne 已率先在产品中实现了 **“Git for Data”** 的核心能力，将数据管理提升到与代码工程同等严谨和高效的水准。对于追求快速迭代和数据可靠性的团队来说，这意味着数据试验可以像代码开发一样“放手一搏”——因为只要在 MatrixOne 中打好快照、开好分支，任何改动都可以追踪、有据可依，错误也可以瞬间撤销。

在 MatrixOne 里面你可以把它理解为：

> **Snapshot ≈ commit 点**，**Branch ≈ 分支引用**，**Diff/Merge/Restore ≈ Git 操作**，而且一切都是 SQL。

---

## 4. 一套完整可跑的 SQL：base → snapshot → branch → dml → diff/merge → restore

### 4.1 初始化：base 表 + 初始数据

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

### 4.2 第一次“存档”：给 base 打快照（sp_v1）

```sql
create snapshot sp_orders_v1 for table demo_branch orders_base;
show snapshots;
```

### 4.3 从快照拉两条分支：风险修复线 & 活动实验线

```sql
data branch create table orders_riskfix from orders_base{snapshot='sp_orders_v1'};
data branch create table orders_promo  from orders_base{snapshot='sp_orders_v1'};
```

### 4.4 在分支上随便改：互不干扰

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

### 4.5 给分支头再打快照：为了可重复 diff

```sql
create snapshot sp_riskfix for table demo_branch orders_riskfix;
create snapshot sp_promo   for table demo_branch orders_promo;
```

### 4.6 Diff：两条分支到底改了什么？

```sql
data branch diff orders_riskfix{snapshot='sp_riskfix'}
  against orders_promo{snapshot='sp_promo'};
```

### 4.7 Merge：把“正确的改动”合并回主干

```sql
-- 先把风险修复合回主干
data branch merge orders_riskfix into orders_base;

-- 再尝试把促销合回主干（冲突策略示例）
data branch merge orders_promo into orders_base when conflict skip;
-- data branch merge orders_promo into orders_base when conflict accept;
```

### 4.8 不满意？Restore：把主干一键回到 sp_orders_v1

```sql
restore database demo_branch table orders_base from snapshot sp_orders_v1;
```

---

## 5. 一张图看懂：数据血缘 + diff/merge/restore
![mermaid diagram](./pics/mermaid.svg)

---

## 6. 进阶玩法：Branch 也是“数据同步/跨集群同步”的基础设施

很多人第一次看到 Data Branch，会把它理解成“数据研发协作工具”。  
但它还有一个更工程化的隐藏技能：**把 diff 变成可携带、可重放的“数据补丁（patch）”**。

一句话：  
> **在源集群算出 diff → 把 diff 输出成文件（本地或 stage）→ 在任意地方重放（apply）这份 diff。**

这对“跨集群同步 / 预发环境刷新 / 灾备演练 / 分布式团队协作”特别有用。

### 6.1 Diff 输出成文件：CSV（全量初始化）或 SQL（增量补丁）

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

### 6.2 跨集群同步：把 diff 写到 stage（例如 S3），另一端拉取并重放

你可以把 diff 输出到 stage（例如 S3）：

```sql
-- 源集群：创建一个外部 stage（S3 / 兼容 S3 的对象存储）
create stage stage01 url =
  's3://bucket/prefix?region=cn-north-1&access_key_id=xxx&secret_access_key=yyy';

-- 源集群：把 diff 直接写到 stage
data branch diff t1 against t2 output file 'stage://stage01/';
```

输出会告诉你保存到了哪个文件，并提示该 SQL 补丁主要由 `DELETE FROM ...`、`REPLACE INTO ...` 组成。

### 6.3 “Apply diff” 怎么做？（本质是重放生成的 SQL / 导入生成的 CSV）

这里有个非常务实的点：**MatrixOne 目前并不是再提供一条 `APPLY DIFF` 的新语句**，而是把 diff 产物变成“你能在任何地方执行/导入”的标准文件：

- 如果 diff 输出的是 **`.sql`**：在目标集群执行该 SQL 文件即可（相当于 apply patch）  
- 如果 diff 输出的是 **`.csv`**：在目标集群把 CSV 导入到目标表即可（相当于 bootstrap）

#### 6.3.1 重放 SQL diff（增量同步）

你可以用任意 MySQL 客户端方式执行 SQL 文件（因为 MatrixOne 兼容 MySQL 协议）。此外，MatrixOne 生态工具 `mo_ctl` 也支持直接执行 SQL 文件。

```bash
# 方式 A：用 mysql 客户端重放（示意）
mysql -h <mo_host> -P <mo_port> -u <user> -p <db_name> < diff_xxx.sql

# 方式 B：使用 mo_ctl 重放（示意）
mo_ctl sql diff_xxx.sql
```

#### 6.3.2 导入 CSV diff（初始化同步）

MatrixOne 支持 `LOAD DATA`，并且文件来源可以是本地文件或 `stage://...`。

```sql
load data local infile '/tmp/diffs/diff_xxx.csv'
into table demo_branch.orders_base
fields enclosed by '"' escaped by '\\' terminated by ','
lines terminated by '\n';
```

如果 CSV 在 stage 上，也可以用 `LOAD DATA INFILE 'stage://stage_name/path' ...` 的方式直接从对象存储导入。

---

## 7. 行业视角：数据分支理念的实践与探索
MatrixOne 不是孤军奋战。近年来，“数据分支”理念在业界逐渐兴起，不少国内外公司和开源项目都在探索类似的版本化数据管理方案。下面列举几个具有代表性的案例：
* Dolt – Git式版本数据库：Dolt 是一个将 Git 功能用于关系数据库的开源项目，其定位直截了当——“Git for Data”。作为一个 MySQL 风格的数据库，Dolt 允许用户像对待 Git 仓库一样，对数据进行克隆、Fork、创建分支以及合并。开发者可以为数据开启分支，发起 pull request 来审阅和合并数据修改。这使得数据协作流程与代码协作流程融为一体，例如先在分支上修复数据错误，测试无误后再合并到主库。Dolt 的出现证明了在关系型数据库中引入版本控制的可行性和价值。
* PlanetScale – 云端 MySQL 的数据分支：PlanetScale 是一家提供托管 MySQL 服务的公司，其早期就支持数据库分支功能，不过起初仅支持 schema（模式）的分支。近期他们推出了 **Data Branching®** 功能，允许创建包含完整数据和模式的隔离副本。也就是说，开发者可以在 PlanetScale 上瞬间“拷贝”出生产库的完整数据用于开发测试，而不用担心影响生产环境。值得注意的是，这种数据分支在底层也是零拷贝实现，新分支最初并不占用额外存储，除非有写入发生。PlanetScale 将这一能力注册了商标（Data Branching®），足见其对该功能的重视程度，也反映出业界对数据库完全分支隔离需求的认可。
* Neon – 支持分支的云原生 Postgres：Neon 是新兴的云端 PostgreSQL 服务，它将 Postgres 的存储层重构，以支持即时克隆和分支。Neon 开创性地引入了写时复制（Copy-on-Write）机制用于数据分支，让团队能够在数秒内生成生产数据库的完整拷贝用于开发测试。每当需要新环境时，只需创建一个分支即可得到完整数据快照，而无需等待冗长的备份恢复过程。Neon 的这种“代码般”的数据库分支理念在近年广受关注，证明在传统数据库上通过架构创新也能实现数据分支的高效机制。
* Supabase – 与应用开发流程集成的分支：Supabase 是另一个基于 PostgreSQL 的后端服务平台。它将数据库分支与应用的 Git 工作流相集成，实现了 **“每个功能分支对应一个数据库实例”的预览环境。当开发者在代码仓库中新建功能分支时，可同步生成对应的数据库分支，用于该功能的独立测试环境。这种模式下，每个功能的代码和数据变更都在隔离沙盒中运行，确保在功能合并上线前不会影响主库。Supabase 当前的实现是模式分支** 为主，即新分支继承生产库的schema但不自动复制数据，需要通过种子脚本填充测试数据。尽管如此，它充分说明了数据分支概念在开发者工具链中的价值：数据库可以跟随代码一起“分支-测试-合并”，从而实现完整的全栈预演。
* TerminusDB – 图数据库的版本控制：数据分支的不仅应用于关系型或表格数据，在图数据库和知识库领域也有所尝试。以开源的 TerminusDB 为例，它是一个原生支持版本控制的图形/文档数据库，被称为 **“架构上类似 Git 的数据库”。TerminusDB 支持对JSON文档数据进行branch、merge、squash、reset** 等操作，甚至提供了类似 Git 的 commit graph、diff、log 等功能。通过这些机制，不同的人可以在同一个知识图谱数据上创建分支、各自演进，然后合并变更。这种“Git化”的数据协作使得构建版本化的知识图谱成为可能，适合需要追踪数据演化历史、实现数据产品版本迭代的场景。
* lakeFS – 面向数据湖的Git化版本管理：在大数据和数据湖领域，也出现了类似 Git 的版本控制方案。开源项目 lakeFS 提供了针对对象存储（如 Amazon S3）的数据版本控制，它的核心就是分支+提交的模型。数据团队可以通过 lakeFS 为数据湖中的数据创建分支，进行试验性的数据处理，而不影响主数据仓库。lakeFS 实现了零拷贝分支（Zero-copy branch），使得在不实际复制文件的情况下就能让开发者得到隔离的测试环境。例如，数据科学家可以基于生产数据创建一个分支来清洗数据、处理异常，确认质量后再将高质量数据合并入主分支。同时，每次分支上的提交都会形成一个 commit，支持审计和快速回滚。可以认为，lakeFS 将 Git for Data 理念带到了数据湖/大数据领域，为机器学习流水线的数据管理提供了强有力的工具。

综上可见，无论是数据库领域的 MatrixOne、Dolt、Neon，还是大数据领域的 lakeFS，数据分支/版本化正成为行业趋势。国外的开源项目和云服务提供商纷纷投入其中，国内团队也开始关注这一方向。MatrixOne 的探索使其成为国内少数实践“Git化数据管理”的先行者。这些案例共同证明，将分支理念用于数据管理并非天马行空：随着数据规模和协作复杂度的提升，工程化的版本控制将是必然选择。数据分支功能为企业带来的价值也是显而易见的——更快的试验迭代、更高的数据质量保障、跨团队协作的标准化，以及出现事故时更从容的应对。


## 8. 写在最后：Branch 不是“功能”，而是一种数据工程方法论

“Branch”功能的出现，为长期困扰数据工作的诸多难题提供了一剂良方。有人将 Git for Data 称作数据管理领域的革命性范式，因其巧妙融合了声明式的数据管理理念和 **“数据即代码”** 的思维，并引入了类似 Git 的强大版本控制能力。这种创新从根本上改变了数据管理的方式，使之更灵活、可控、高效。对于现代 AI 和大数据应用来说，数据分支带来了全新的解决思路：不仅有效保障了数据的质量与安全，更显著提升了数据的一致性和开发协作效率

当你把 Data Branch 用在真实项目里，它会自然长成一套流程：

- **每次大规模清洗/标注/特征工程**：先打 snapshot，再开分支  
- **每个算法实验**：一人一分支（甚至每个 PR 一分支）  
- **上线前**：diff + review + merge（像 code review 一样 review 数据变更）  
- **跨集群/跨环境**：diff 输出成文件（本地或 stage）→ 在目标集群重放（apply）  
- **出事故**：restore 回健康快照，先止血，再排查

展望未来，我们有理由相信数据分支将成为数据基础设施的标配能力之一。就像软件开发无法想象没有版本控制的协作，当数据管理变得和代码管理一样严谨且可追溯时，数据不再是沉睡的负担，而将成为敏捷创新的加速器。MatrixOne 等产品所践行的这一变革，正帮助企业将数据资产转化为可以像代码一样灵活运用的竞争力。对于身处 AI 时代的企业来说，引入“Git for Data”无疑是构建核心数据竞争力的关键一步：让数据的每一次演进都有迹可循，让每一次试验都无后顾之忧，让创新迭代快人一步。数据管理的未来已经到来，而分支功能正是开启未来大门的钥匙。

---

## 参考链接
* [MatrixOrigin, “AI 时代的数据管理新范式：Git for Data 让数据工程化”, InfoQ 写作社区](https://xie.infoq.cn/article/50d702e4a50b8168ea0a71fb5#:~:text=1)
* [“Git for Data: 像 Git 一样管理你的数据”, InfoQ 中国](https://www.infoq.cn/article/8qjz1tqen5qngx3g8ofu#:~:text=MatrixOne%20%E5%B7%B2%E5%85%B7%E5%A4%87%20Git%20for%20Data,%E7%9A%84%E6%A0%B8%E5%BF%83%E8%83%BD%E5%8A%9B%EF%BC%8C%E5%8C%85%E6%8B%AC%EF%BC%9A)
* [Alex Francoeur, “PostgreSQL Branching: Xata vs. Neon vs. Supabase – Part 1”, Xata 官方博客](https://xata.io/blog/neon-vs-supabase-vs-xata-postgres-branching-part-1#:~:text=Neon%20popularized%20the%20idea%20of,our%20new%20PostgreSQL%20platform%20at)
* [Sergio De Simone, “以 Git 为数据源、具备版本控制的数据库 Dolt 新增了 PostgreSQL 风味”, InfoQ 中国](https://www.infoq.cn/article/4862sxkdtgxglaojd3bd#:~:text=Dolt%20%E4%BD%9C%E4%B8%BA%20SQL%20%E6%95%B0%E6%8D%AE%E5%BA%93%EF%BC%8C%E5%85%81%E8%AE%B8%E7%94%A8%E6%88%B7%E5%83%8F%E6%98%AF%20Git,%E4%BB%A3%E7%A0%81%E5%BA%93%E4%B8%80%E6%A0%B7%E8%BF%9B%E8%A1%8C%E5%85%8B%E9%9A%86%E3%80%81fork%E3%80%81%E5%88%86%E6%94%AF%E5%8F%8A%E5%90%88%E5%B9%B6%E3%80%82%E9%80%9A%E8%BF%87%20Dolt%EF%BC%8C%E5%BA%94%E7%94%A8%E7%A8%8B%E5%BA%8F%E5%BC%80%E5%8F%91%E8%80%85%E5%8F%AF%E4%BB%A5%E4%B8%BA%E7%94%A8%E6%88%B7%E5%88%9B%E5%BB%BA%E5%88%86%E6%94%AF%EF%BC%8C%E5%90%88%E5%B9%B6%E5%B7%A5%E4%BD%9C%E6%B5%81%EF%BC%8C%E6%AF%94%E5%A6%82%E5%8F%91%E9%80%81%20pull%20%E8%AF%B7%E6%B1%82%E4%BF%AE%E5%A4%8D%E6%95%B0%E6%8D%AE%E4%B8%AD%E7%9A%84%E9%94%99%E8%AF%AF%E3%80%82%E5%90%8C%E7%90%86%EF%BC%8CDolt%20%E5%8F%AF%E4%BB%A5%E9%80%9A%E8%BF%87%E6%95%B0%E6%8D%AE%E5%BA%93%E5%88%86%E6%94%AF%E3%80%81%E5%8F%98%E6%9B%B4%E5%BA%94%E7%94%A8%EF%BC%8C%E5%9C%A8%E6%9A%82%E5%AD%98%E7%8E%AF%E5%A2%83%E4%B8%AD%E6%B5%8B%E8%AF%95%EF%BC%8C%E5%B9%B6%E6%9C%80%E7%BB%88%E9%83%A8%E7%BD%B2%E5%9B%9E%E7%94%9F%E4%BA%A7%E7%8E%AF%E5%A2%83%E7%9A%84%E8%BF%99%E7%A7%8D%E7%AE%80%E5%8D%95%E6%A8%A1%E5%9E%8B%E4%BF%AE%E6%94%B9%E7%94%9F%E4%BA%A7%E6%95%B0%E6%8D%AE%E5%BA%93%E3%80%82)
* [PlanetScale 文档, “Data Branching®”](https://planetscale.com/docs/vitess/schema-changes/data-branching#:~:text=Overview)
* [TerminusDB 维基百科](https://en.wikipedia.org/wiki/TerminusDB#:~:text=TerminusDB%20is%20an%20open%20source,Engines)
* [lakeFS 官方网站, “Data Collaboration & Data Branching”](https://lakefs.io/#:~:text=Data%20Collaboration)
* [MatrixOne CREATE SNAPSHOT](https://docs.matrixorigin.cn/v25.3.0.0/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/create-snapshot/)
* [MatrixOne RESTORE SNAPSHOT](https://docs.matrixorigin.cn/v25.3.0.0/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/restore-snapshot/)
* [MatrixOne LOAD DATA](https://docs.matrixorigin.cn/en/dev/MatrixOne/Reference/SQL-Reference/Data-Manipulation-Language/load-data-infile/)
