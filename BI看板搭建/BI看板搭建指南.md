# BI 看板搭建指南（影眸 BI 实习生 · 4 小时版）

> 目标：**今天之内**，用你已经清洗好的数据集做出一个能截图进简历的 BI 看板，
> 并附上一份数据字典 + 指标口径说明。
>
> 为什么用这个项目：你不缺数据，缺的只是"把它放进 BI 工具里"这一步。
> 而且这个看板讲的故事是——**"我当初三个数据假设，是怎么被数据推翻的"**。
> 这比"我学了个 BI 工具"值钱得多。

---

## 第 0 步：先建立 5 个概念（10 分钟，不用碰工具）

| 概念 | 一句话 | 你已有的对应物 |
|---|---|---|
| **数据集 Dataset** | 一张能被拖拽的宽表，或一段 SQL 查询结果 | 你写的 Spark SQL |
| **维度 Dimension** | 用来切分的字段：类别、日期、长度分桶 | `cluster`、`length_bucket` |
| **度量 Measure** | 被计算的数值：条数、占比、平均长度 | `COUNT(*)`、`AVG(output_len)` |
| **过滤器 Filter** | 全局筛选条件 | `WHERE` |
| **仪表板 Dashboard** | 把多个图表拼在一页 | 你 README 的"需求实现"章节 |

**再加一条最重要的——指标口径（Metric Definition）**：
"新增用户"到底怎么算、"占比"的分母是谁。**BI 岗 80% 的争执都在这一层。**

> 你项目里已经有过一次：为了判断"该不该降采样"，你先定义了六类的口径，再实测占比。
> **那件事的 BI 名字就叫"指标口径落地"。**

---

## 第 1 步：装 Metabase（30 分钟）

### 为什么选 Metabase 而不是 Superset

| | Metabase | Superset |
|---|---|---|
| 装法 | **一个 jar 包**，`java -jar metabase.jar` | Docker（推荐）或 Python 3.11 venv 编译 |
| 你有 Java 吗 | **有**（Spark 需要的，你早配好了） | 不需要但依赖多 |
| 上手时间 | **10 分钟** | 1-2 小时（容易卡在依赖编译） |
| 简历认可度 | JD 名单里有它 | 更"正统" |

**先跑通 Metabase，再看要不要补 Superset。** 反正 JD 里两个都认。

### 具体步骤

**① 确认 Java 版本 ≥ 11**

```powershell
java -version
```

**② 下载 Metabase**

去 https://www.metabase.com/start/oss/ 下载 `metabase.jar`（约 400 MB），
放到一个专门的目录，例如 `E:\bi\metabase.jar`。

**③ 启动**

```powershell
cd E:\bi
java -jar metabase.jar
```

看到 `Metabase Initialization COMPLETE` 就成了。**第一次启动会初始化数据库，需要 2-5 分钟**，
之后每次启动几秒。

**④ 打开浏览器**

```
http://localhost:3000
```

第一次会让你建管理员账号（随便填，本地用）——**把用户名密码记下来**。

---

## 第 2 步：准备 BI 用的数据（1 小时）

### 为什么不能直接用你现在的输出

你 `train_2M_CN_filter/output/sft_dataset` 里**只有 `instruction` 和 `output` 两列**，
没有类别、没有长度、没有分桶。BI 工具靠"维度字段"切分数据——**没维度就画不出图**。

所以要先重跑一遍，加上这些列：

| 字段 | 含义 |
|---|---|
| `cluster` | 类别（代码生成/翻译/改写/抽取/基于文本/问答/其他） |
| `output_len` | output 字数 |
| `length_bucket` | 长度分桶：短(<50) / 中短(50-200) / 中(200-500) / 长(500-2000) / 超长(>=2000) |
| `has_code` | 是否含代码块 ``` （SFT 数据里代码样本值钱） |
| `has_url` | 是否含链接（验证你当初的"图片占比"假设） |
| `has_img` | 是否含 Markdown 图片标记 `![` |

### 跑脚本

我已经写好 `bi_data_prep.py`，放到你 `train_2M_CN_filter/` 目录下跑（路径按你项目结构默认好了）。

**第一步：先 dry-run 验证（不写库，几秒到几分钟）**

```powershell
cd "E:\A homework\2026summer_holiday\spark_note\PySpark\SQL\train_2M_CN_filter"
python bi_data_prep.py --dry-run
```

它会打印：六类分布、长度分桶、长度分位数、含代码/链接/图片的条数、
instruction 前 6 字 Top 10、数据样例。

**⚠️ 把这次输出截图保存**——这些数字要填进后面的数据字典和简历。

**第二步：写库**

```powershell
# 演示用：只写 30 万条，5 分钟以内
python bi_data_prep.py --sample 300000

# 或者全量 186 万条（JDBC 单线程，约 10-30 分钟）
python bi_data_prep.py
```

**写库前先设环境变量**（脚本从环境变量读用户名密码，和你 `writers.py` 一致）：

```powershell
$env:MYSQL_USER="root"
$env:MYSQL_PWD="你的密码"
```

如果报找不到驱动，指定 jar 路径（**你本机的 jar 在 `SQL\` 目录下**）：

```powershell
$env:MYSQL_JDBC_JAR="E:\A homework\2026summer_holiday\spark_note\PySpark\SQL\mysql-connector-java-8.0.13.jar"
```

> 脚本会自动在 `../mysql-connector-java-8.0.13.jar` 和 `./lib/` 下找驱动，
> 找到了会打印 `使用 JDBC 驱动: ...`。没打印这行就说明没找到。

**第三步：验证写进去了**

```powershell
mysql -u root -p -e "SELECT COUNT(*) FROM bigdata.sft_dataset_bi;"
```

或者在 Navicat / DBeaver 里看一眼这张表。

---

## 第 3 步：连上 Metabase（20 分钟）

1. Metabase 右上角 **齿轮 → Admin settings → Databases → Add database**
2. 选 **MySQL**，填：
   - Host：`localhost`，Port：`3306`
   - Database name：`bigdata`
   - Username / Password：你的 MySQL 账号
3. **如果报驱动错误**：Metabase 内置 MySQL 驱动版本较旧，可能连不上 MySQL 8。
   解决办法：下载 `mysql-connector-j-8.0.33.jar`，放到 Metabase 的 `plugins/` 目录，
   重启 Metabase。
   > （你本机那个 8.0.13 的驱动，Spark 能用，Metabase 建议用 8.0.33 以上）
4. 连接成功后，**Data model → 把 `cluster`、`length_bucket` 标记为 Category 类型**，
   把 `output_len` 标为 Number。这一步决定后面能不能拖拽。

---

## 第 4 步：做 Dashboard（1 小时）

### 先定义指标口径（这一步不能跳）

写下来，后面所有图都按这个算：

| 指标 | 口径 |
|---|---|
| 总条数 | 清洗去重后的最终条数 = 1,863,135 |
| 去重减少条数 | 清边界后 1,966,152 − 去重后 1,863,135 = 103,017 |
| 整体过滤率 | (2,000,000 − 1,863,135) / 2,000,000 = **6.8%** |
| 类别占比 | 该类条数 / 总条数 |
| 重复 output 组数 | 15,582 —— **注意这是"组数"不是"条数"** |

**最后那一行是重点**。面试时你主动说"我一开始把这个数当条数用，后来发现是组数，
两者混用会得出完全不同的结论"——**这一句话就证明你懂"口径歧义"**，
而这是 BI 岗最看重的，和工具完全无关。

### 要做 4 张图（每张都有明确的"回答什么问题"）

**图 1：指标卡（3 个）**
- 总条数：1,863,135
- 去重减少：103,017 条
- 整体过滤率：6.8%

> 作用：看板第一眼要能看到"这份数据是什么规模"。
> Metabase 里用 **Question → Number** 做。

**图 2：柱状图 · 六类分布**
- 维度（X 轴）：`cluster`
- 度量（Y 轴）：`COUNT(*)`
- 排序：按条数降序

> 回答的问题：**"各类别是否失衡？"** —— 你当初假设"严重失衡"，实测是 5.2%~29.1%。
> Metabase 里用 **Question → Bar**，还可以把占比打开（Visualization → Show values）。

**图 3：柱状图 · 长度分桶**
- 维度：`length_bucket`
- 度量：`COUNT(*)`

> 回答的问题：**"长度是否悬殊？"** —— 你当初假设"有截断"，实测被否证。

**图 4：横向条形图 · instruction 前 6 字 Top 10**
- 需要先在 Metabase 里建一个 SQL 问题（Native query）：

```sql
SELECT SUBSTRING(instruction, 1, 6) AS prefix,
       COUNT(*) AS cnt
FROM sft_dataset_bi
GROUP BY SUBSTRING(instruction, 1, 6)
ORDER BY cnt DESC
LIMIT 10;
```

> 回答的问题：**"模板化到底严重不严重？"** —— 这是你第三个假设的实体证据。
> **这张图是全看板最有说服力的一张**，因为它把"模板化"从一个形容词变成了一个可见的形状。

**图 5（加分）：含代码 / 链接 / 图片标记的条数**

```sql
SELECT '含代码块' AS 类型, SUM(has_code) AS 条数 FROM sft_dataset_bi
UNION ALL SELECT '含链接', SUM(has_url) FROM sft_dataset_bi
UNION ALL SELECT '含图片标记', SUM(has_img) FROM sft_dataset_bi;
```

> 回答的问题：**"图片占比高"这个假设成立吗？** —— 直接给出反证数字。

### 拼 Dashboard

1. **New → Dashboard**，命名：`中文指令数据集质量看板`
2. 把上面 5 张图拖进去，按这个顺序排：
   - 第一行：3 个指标卡
   - 第二行：六类分布 + 长度分桶
   - 第三行：模板化 Top 10（横跨整行）
   - 第四行：含代码/链接/图片
3. **加一个全局过滤器**：按 `cluster` 或 `length_bucket` 筛选
4. **给每张图加描述**（点图 → Edit → Description），写清楚"这张图回答什么问题、
   口径是什么"——**这一步就是在做"看板使用说明"，JD 第 6、9 条要求的东西**
5. **截图**：整页截一张，存好

---

## 第 5 步：写数据字典 + 口径说明（30 分钟）⭐

**这一步比看板本身更值钱**——影眸 JD 里"数据字典、指标说明、口径文档"被单独列了一条。

新建一个 markdown 文件（或者 Metabase 里的一个 Dashboard 页面），照抄改：

```markdown
# 中文指令数据集质量看板 · 数据字典与指标口径

## 一、数据来源
BelleGroup/train_2M_CN（HuggingFace），原始约 200 万条、1.8GB。
经边界清洗（output 长度 ≥ 10）与按 output 去重后，产出 1,863,135 条。

## 二、字段说明
| 字段 | 类型 | 含义 | 口径 |
|---|---|---|---|
| cluster | 维度 | 指令类别 | 关键词规则打标，六类；「其他」占 29.1%，精度有限，**未做人工校验** |
| instruction | 文本 | 指令 | 原始字段，未做改写 |
| output | 文本 | 回答 | 原始字段，未做改写 |
| output_len | 度量 | 回答字数 | = LENGTH(output)，按字符数计（中文一个字算 1） |
| length_bucket | 维度 | 长度分桶 | <50 短 / 50-200 中短 / 200-500 中 / 500-2000 长 / ≥2000 超长 |
| has_code | 度量 | 是否含代码块 | output 中是否出现 ```；1 是，0 否 |
| has_url | 度量 | 是否含链接 | output 中是否出现 http |
| has_img | 度量 | 是否含图片标记 | output 中是否出现 ![ |

## 三、指标口径
- **总条数**：清洗去重后的最终条数 = 1,863,135
- **去重减少条数**：1,966,152（清边界后）− 1,863,135（去重后）= 103,017
- **整体过滤率**：(2,000,000 − 1,863,135) / 2,000,000 = 6.8%
- **类别占比**：该类条数 ÷ 总条数
- **重复 output 组数**：15,582

## 四、已知限制与口径陷阱
1. 「重复 output 15,582」是**组数不是条数**，不可与条数直接相加；
   该批重复实际涉及约 12.2 万条记录。
2. `cluster` 用关键词匹配，精度有限，「其他」类占 29.1%，未做人工校验——
   因此**不建议基于该字段做任何"砍掉某一类"的决策**。
3. 长度按**字符数**统计，不是 token 数；如需 token 口径需另接 tokenizer。
4. 数据仍存在模板化现象，但未做降采样，**保留原始分布**。

## 五、结论（这个看板回答了什么）
- 六类分布 5.2% ~ 29.1%，**不存在单一类别主导** → 原定的降采样方案失去依据，已放弃
- 实测三个数据质量假设（长度截断 / 模板化严重 / 图片占比高）**全部被数据否证**
```

**注意第四部分**——主动写"已知限制"和"口径陷阱"是加分项。
它证明你清楚自己的数据哪里不牢，而这正是 JD 第 2 条"通过数据治理手段保障数据质量"要的东西。

---

## 第 6 步：写进简历（10 分钟）

在项目一里加一条 bullet：

```
- **数据可视化**：基于 Metabase 搭建数据集质量看板（总条数 / 类别分布 / 长度分布 /
  模板化 Top10 / 多模态标记占比），输出指标口径说明与数据字典，明确「其他」类
  占比 29.1% 等已知限制
```

技能栏加一行：

```
- **BI 工具**：Metabase（数据集配置、维度建模、看板搭建、指标口径与数据字典沉淀）
```

> ⚠️ **只写你真做过的**。做完这个看板，上面每一个词你都撑得住。

---

## 第 7 步：面试怎么说（背下来）

被问到"你没有 BI 经验吧"：

> "我确实没有 BI 岗位的实习经历。但我给自己那份 186 万条的数据集做过一套质量看板——
> 定义了六类的分类口径、长度分桶口径、去重率的计算方式，用 Metabase 出了分布、
> 长度、模板化三类图，并写了数据字典。
>
> **我理解 BI 的核心不是工具，是口径。** 我在清洗数据时踩过这个坑：一开始我以为
> '重复 15,582'是条数，后来发现统计出来的是**组数**，那批重复实际涉及 12.2 万条。
> 这两个数混用，会得出完全不同的结论。"

**没有一个字是编的，而且它证明的正是岗位最需要的东西。**

---

## 排错速查

| 现象 | 原因 | 解决 |
|---|---|---|
| `java -jar metabase.jar` 报找不到主类 | Java 版本 < 11 | 装 JDK 11/17（你 Spark 用的那个应该够） |
| Metabase 连 MySQL 报驱动错误 | 内置驱动旧 | 下载 `mysql-connector-j-8.0.33.jar` 放 `plugins/` 重启 |
| Metabase 连接超时 | MySQL 没启动 | `net start mysql`（或服务里启动） |
| 写库报 `Access denied` | 环境变量没设 | `$env:MYSQL_USER` / `$env:MYSQL_PWD` |
| 写库报找不到驱动 | jar 路径不对 | 设 `$env:MYSQL_JDBC_JAR` 指到绝对路径 |
| 写库很慢 | JDBC 单线程 | 用 `--sample 300000`，演示够用 |
| 图拖不出来 | 字段类型没标 | Data model 里把 cluster / length_bucket 标成 Category |
| Spark 报 HADOOP_HOME 错 | WinUtils 没配 | 你之前配好了，用的是 `E:\hadoop` |

---

## 时间表

| 时间 | 做什么 | 产出 |
|---|---|---|
| 第 0-1 步（40 min） | 建概念 + 装 Metabase | 能打开 `localhost:3000` |
| 第 2 步（60 min） | 跑 `bi_data_prep.py` | MySQL 里有 `sft_dataset_bi` 表 |
| 第 3 步（20 min） | 连数据库 | Metabase 里能看到表 |
| 第 4 步（60 min） | 出 5 张图 + 拼看板 | **一张能截图的看板** |
| 第 5 步（30 min） | 写数据字典 | 一页文档 |
| 第 6 步（10 min） | 改简历 | 简历 +1 条 |
| **合计** | **约 4 小时** | **"没有 BI 经验" → "有一个 BI 项目"** |

---

## 最后一句

**这个岗位你可能拿不到**——你会跟已经在 BI 岗实习过的人竞争，这个差距一周补不平。

但你要清楚这 4 小时买到的是什么：
1. **简历上多一个和 JD 直接对应的项目**（从"完全没有"变成"做过"）
2. **一份通用的装备**——BI 和数据分析岗的面试题重合度极高，这套东西转手就能投别的岗
3. **一个真实的市场校准**：投出去，看筛不筛得过，比再问五个 AI 有用

**做完就去投。别做到"完美"再投——那个时间点永远不会来。**
