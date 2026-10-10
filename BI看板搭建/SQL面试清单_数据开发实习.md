# SQL 面试手撕清单（数据开发 / 数据分析实习）

> **用法**：不要读，要**敲**。打开你本地的 MySQL，建下面这张表，把 12 道题全部手打一遍。
> 每道题先自己写 5 分钟，写不出来再看答案——**看答案等于没练**。
>
> **为什么用你自己的数据当例子**：面试官问"你去重怎么写"，你能同时说出"我简历里那个
> 186 万条的项目就是这么干的"。**答案 + 真实经历，才是加分项；只有答案，只是及格。**

---

## 0. 先建一张练习表（和你简历上的数据同构）

```sql
CREATE TABLE `instruction_data` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
  `instruction` VARCHAR(500) NOT NULL                COMMENT '指令',
  `output`      TEXT         NOT NULL                COMMENT '回答',
  `category`    VARCHAR(20)  DEFAULT NULL            COMMENT '类别',
  `create_date` DATE         DEFAULT NULL            COMMENT '写入日期',
  `word_count`  INT          DEFAULT 0               COMMENT 'output 字数',
  PRIMARY KEY (`id`),
  KEY `idx_output` (`output`(255)) COMMENT '去重时按 output 分组要用到'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中文指令数据集';
```

⚠️ **顺手记一个面试点**：`TEXT` 类型不能直接建索引，所以上面用的是 `output(255)` 前缀索引。
面试官问"为什么加 (255)"，你要能答"TEXT 全文太长，前缀索引省空间且够用"。

---

## 1. ★☆☆ 基础：分组统计 + 过滤

**题目**：统计每个类别的数据条数，并按条数从多到少排序，只显示条数大于 1000 的类别。

```sql
SELECT category,
       COUNT(*) AS cnt
FROM instruction_data
GROUP BY category
HAVING COUNT(*) > 1000
ORDER BY cnt DESC;
```

**坑**：`WHERE` 和 `HAVING` 的区别——`WHERE` 在分组**前**过滤行，`HAVING` 在分组**后**过滤组。
上面这题只能用 `HAVING`。**这是面试最爱问的第一题。**

---

## 2. ★★☆ 你项目里的真事：分组去重，每组只保留一条

**题目**：`output` 相同的数据只保留 `id` 最小的那一条，统计去重后还剩多少条。

```sql
-- 写法一：窗口函数（推荐，通用性最强）
SELECT COUNT(*) AS cnt_after_dedup
FROM (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY output ORDER BY id) AS rn
    FROM instruction_data
) t
WHERE rn = 1;
```

```sql
-- 写法二：自连接 / 半连接（不用窗口函数时的写法）
SELECT COUNT(*)
FROM instruction_data a
WHERE a.id = (SELECT MIN(b.id) FROM instruction_data b WHERE b.output = a.output);
```

**必须能说出来**：`ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` 的区别。

| 函数 | 遇到并列时 |
|---|---|
| `ROW_NUMBER()` | 1, 2, 3, 4（强行不并列） |
| `RANK()` | 1, 2, 2, 4（跳号） |
| `DENSE_RANK()` | 1, 2, 2, 3（不跳号） |

🎯 **这题就是你的项目**。答完补一句：

> "我在一个 200 万条的数据集上做过同样的事，实测有 15,582 组 output 重复。
> 我当时用的是 Spark 的 `groupBy` + `first`，思路和这个 `ROW_NUMBER()` 是一样的。"

---

## 3. ★★☆ 找出重复的数据

**题目**：列出所有重复出现的 `output` 及其出现次数（这就是你打出 15582 的那条查询）。

```sql
SELECT output,
       COUNT(*) AS repeat_cnt
FROM instruction_data
GROUP BY output
HAVING COUNT(*) > 1
ORDER BY repeat_cnt DESC;
```

**面试官可能追问**："如果 output 特别长，直接 GROUP BY 会有什么问题？"
→ 答：内存和 shuffle 开销大；实际生产里会先取哈希（如 `MD5(output)`）再分组。

---

## 4. ★★☆ 条件聚合 / 行转列

**题目**：一行输出六个类别的条数（六列）。

```sql
SELECT
    SUM(CASE WHEN category = '写作'   THEN 1 ELSE 0 END) AS cnt_writing,
    SUM(CASE WHEN category = '问答'   THEN 1 ELSE 0 END) AS cnt_qa,
    SUM(CASE WHEN category = '翻译'   THEN 1 ELSE 0 END) AS cnt_translate,
    SUM(CASE WHEN category = '改写'   THEN 1 ELSE 0 END) AS cnt_rewrite,
    SUM(CASE WHEN category = '摘要'   THEN 1 ELSE 0 END) AS cnt_summary,
    SUM(CASE WHEN category = '其他'   THEN 1 ELSE 0 END) AS cnt_other
FROM instruction_data;
```

也可以用 `COUNT(CASE WHEN ... THEN 1 END)`——`COUNT` 不计 NULL，写法更短。
**面试点**：MySQL 没有 `PIVOT`，行转列只能靠条件聚合。

---

## 5. ★★☆ 分组内 TopN

**题目**：每个类别里，`word_count` 最大的 3 条记录。

```sql
SELECT category, instruction, word_count
FROM (
    SELECT category, instruction, word_count,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY word_count DESC) AS rn
    FROM instruction_data
) t
WHERE rn <= 3;
```

**坑**：不能写成 `GROUP BY category ORDER BY word_count DESC LIMIT 3`——那是全局 TopN，
不是**每组** TopN。**这是最高频的窗口函数考题**，务必手打三遍。

---

## 6. ★★★ 连续问题（经典中的经典）

**题目**：找出**连续 3 天**都有数据写入的日期区间。

```sql
SELECT MIN(create_date) AS start_date,
       MAX(create_date) AS end_date,
       COUNT(*)         AS days_cnt
FROM (
    SELECT DISTINCT create_date,
           DATE_SUB(create_date, INTERVAL ROW_NUMBER() OVER (ORDER BY create_date) DAY) AS grp
    FROM instruction_data
) t
GROUP BY grp
HAVING COUNT(*) >= 3;
```

**核心思路**：**日期减去行号，连续的日期会得到同一个值** —— 这是"连续 N 天"类问题的通用解。
变体：连续登录、连续下单、连续涨幅。**会了这一题，这一整类都会了。**

---

## 7. ★★☆ 累计求和（running total）

**题目**：按日期统计每天的数据量，并计算累计到当天的总量。

```sql
SELECT create_date,
       COUNT(*) AS daily_cnt,
       SUM(COUNT(*)) OVER (ORDER BY create_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM instruction_data
GROUP BY create_date
ORDER BY create_date;
```

**面试点**：`ROWS` 和 `RANGE` 的区别（默认是 `RANGE`，遇到并列值行为不同）。

---

## 8. ★★☆ 占比 / 分布（你项目里的 29.1% 就是这道）

**题目**：算出每个类别的条数及其占总数的百分比，保留两位小数。

```sql
SELECT category,
       COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM instruction_data), 2) AS pct
FROM instruction_data
GROUP BY category
ORDER BY pct DESC;
```

🎯 答完补一句："我项目里就是用这个查出六类分布是 5.2% ~ 29.1%，'其他'占 29.1%，
说明我的关键词分类精度不够——这是我在文档里写明的已知限制。"
**主动暴露自己的短板，是可信度最强的信号。**

---

## 9. ★★☆ 同比 / 环比

**题目**：按月份统计条数，并算出环比增长率。

```sql
SELECT month,
       cnt,
       LAG(cnt) OVER (ORDER BY month) AS prev_cnt,
       ROUND((cnt - LAG(cnt) OVER (ORDER BY month)) * 100.0
             / LAG(cnt) OVER (ORDER BY month), 2) AS growth_pct
FROM (
    SELECT DATE_FORMAT(create_date, '%Y-%m') AS month, COUNT(*) AS cnt
    FROM instruction_data
    GROUP BY DATE_FORMAT(create_date, '%Y-%m')
) t
ORDER BY month;
```

`LAG` / `LEAD` 取前后行——同比换成 `LAG(cnt, 12)` 即可。

---

## 10. ★★★ 多表关联 + 找出差异（对账思维）

**题目**：有两张表 `src_a` 和 `src_b`，找出只在 A 里存在、不在 B 里的记录。

```sql
-- 写法一：LEFT JOIN + IS NULL（最常用）
SELECT a.id, a.output
FROM src_a a
LEFT JOIN src_b b ON a.id = b.id
WHERE b.id IS NULL;
```

```sql
-- 写法二：NOT EXISTS（有索引时通常更快）
SELECT a.id, a.output
FROM src_a a
WHERE NOT EXISTS (SELECT 1 FROM src_b b WHERE b.id = a.id);
```

⚠️ **`NOT IN` 的坑**：如果子查询结果里有 NULL，`NOT IN` 会返回空结果集。
**这是面试的送命题，一定要主动提。**

🎯 **这题对标滴滴那个岗位的"财务对账"。**
如果你投了滴滴那类岗位，答完这题要说：

> "我理解对账本质就是这类差异比对——两个数据源在同一个口径下找出不一致，
> 再归因是时间差、口径差还是真错账。我在数据清洗里做过同一逻辑的微观版本。"

---

## 11. ★★★ 留存 / 复购（业务分析必考）

**题目**：统计每个用户在首次出现后的第 2 天是否再次出现（次日留存）。

```sql
SELECT first_date,
       COUNT(DISTINCT user_id)                                   AS new_users,
       COUNT(DISTINCT CASE WHEN d2.user_id IS NOT NULL
                           THEN f.user_id END)                   AS retained_d2
FROM (SELECT user_id, MIN(create_date) AS first_date
      FROM user_behavior GROUP BY user_id) f
LEFT JOIN user_behavior d2
       ON f.user_id = d2.user_id
      AND d2.create_date = DATE_ADD(f.first_date, INTERVAL 1 DAY)
GROUP BY first_date;
```

**思路**：先求每个用户的首次日期，再回表看第 2 天有没有行为。
**面试官问"留存怎么算"，能写出这个就是及格线以上。**

---

## 12. ★★★ 数据倾斜 / 性能（口头题，不写代码）

**题目**：一个 `GROUP BY` 任务跑了 3 小时，怀疑是数据倾斜，你怎么排查和优化？

**标准答法（背下来）**：

1. **确认**：看 Spark UI，如果少数 task 的耗时/数据量远超其他 task，就是倾斜
2. **定位**：找出是哪个 key 造成的——`SELECT key, COUNT(*) ... GROUP BY key ORDER BY 2 DESC LIMIT 10`
3. **优化**：
   - 加盐打散（给热点 key 加随机前缀 → 局部聚合 → 再去前缀全局聚合）
   - 两阶段聚合（先局部聚合再全局聚合）
   - 对小表用 broadcast join，避免 shuffle join
   - 开启自适应执行（AQE）自动处理倾斜分区

🎯 你能补一句真话："我处理的数据量只有 200 万，**没有真正遇到生产级的数据倾斜**，
但我知道排查思路是看 task 耗时分布。" —— **诚实说没做过，比编一个经历安全得多。**

---

## 手撕 SQL 的 5 个临场要点

1. **先问清楚口径**再动手：数据量、有没有 NULL、要不要去重、时间是按天还是按月
   —— 面试官最想看的其实是这个动作
2. **先写思路再写代码**：口述"我打算先分组再排名"，比闷头写更有分
3. **写完检查三件事**：NULL 处理了吗、`GROUP BY` 的字段和 `SELECT` 对得上吗、边界（=1 还是 <=1）
4. **主动说局限**："这个写法在数据量大时会有全表扫描问题，生产里我会……"
5. **不会就说不会**，然后说"我会往哪个方向查"。**卡住硬编，比说不会扣分多。**

---

## 练习顺序（三天能过一遍）

| 天 | 题目 | 目标 |
|---|---|---|
| 第 1 天 | 1、2、3、4 | 把基础写熟，第 2 题要能默写 |
| 第 2 天 | 5、6、7、10 | 窗口函数 + 连续问题 + 差异比对 |
| 第 3 天 | 8、9、11、12 | 分布、同环比、留存、口头题 |

**练完之后，把第 2、3、5、6 题各默写一遍**——这四道覆盖了 80% 的实习面试手撕题。
