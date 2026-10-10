# -*- coding: utf-8 -*-
"""
为 BI 看板准备数据：跑一遍清洗 + 打标签 + 派生维度列，写进 MySQL。

做的事（和你 train_2M_CN_filter/cat.py 完全一致，只多了打标签和派生列）：
  1. 读 input/train_2M_CN.json
  2. 清边界       output 长度 >= 10
  3. 去重         按 output 去重
  4. 打类别标签   cluster（关键词规则）
  5. 派生维度     output_len / length_bucket / has_code / has_url / has_img
  6. 写 MySQL 表  sft_dataset_bi
  7. 同时写一份 parquet 备份（万一 MySQL 出问题不用重跑）

用法：
    python bi_data_prep.py --dry-run              # 只跑前 5 步，打印统计，不写库（先验证）
    python bi_data_prep.py --sample 300000        # 只取 30 万条写入（演示用，快）
    python bi_data_prep.py                        # 全量 186 万条写入（JDBC 单线程，约 10-30 分钟）
"""

import os
import sys
import time
import argparse

# ---------------- 环境变量必须在 import SparkSession 之前设置 ----------------
os.environ["HADOOP_HOME"] = r"E:\hadoop"
os.environ["hadoop.home.dir"] = r"E:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";E:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------- 参数 ----------------
p = argparse.ArgumentParser()
p.add_argument("--input", default=r"../input/train_2M_CN.json", help="原始 json 路径")
p.add_argument("--out", default="./output/sft_dataset_bi", help="parquet 备份路径")
p.add_argument("--table", default="sft_dataset_bi", help="MySQL 表名")
p.add_argument("--sample", type=int, default=0, help=">0 时只取这么多条写库（演示用）")
p.add_argument("--min-len", type=int, default=10, help="output 长度下限")
p.add_argument("--dry-run", action="store_true", help="只统计不写库")

# MySQL 连接信息（和你 utils/writers.py 保持一致，库名 bigdata）
MYSQL_URL = ("jdbc:mysql://localhost:3306/bigdata"
             "?useSSL=false"
             "&allowPublicKeyRetrieval=true"
             "&Unicode=true&characterEncoding=utf8"
             "&serverTimezone=Asia/Shanghai"
             "&rewriteBatchedStatements=true")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PWD  = os.environ.get("MYSQL_PWD", "")
JDBC_JAR   = os.environ.get("MYSQL_JDBC_JAR", r"../mysql-connector-java-8.0.13.jar")

# 自动在几个常见位置找驱动，找不到就让用户用环境变量指定
_CANDIDATES = [
    JDBC_JAR,
    r"./lib/mysql-connector-java-8.0.13.jar",
    r"../lib/mysql-connector-java-8.0.13.jar",
]


def find_jdbc():
    for c in _CANDIDATES:
        if os.path.exists(c):
            return c
    return None


def build_spark():
    jar = find_jdbc()
    b = (SparkSession.builder
         .appName("SFT-BI-DataPrep")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")     # 去重要 shuffle，别设太小
         .config("spark.sql.warehouse.dir", "./bi_warehouse")
         .config("spark.driver.memory", "4g")  # 给 driver 4GB 堆
         .config("spark.sql.shuffle.partitions", "64"))
    if jar:
        print(f"使用 JDBC 驱动: {os.path.abspath(jar)}")
        b = b.config("spark.jars", os.path.abspath(jar))
    else:
        print("[警告] 没找到 MySQL JDBC 驱动，写库会失败。"
              f"请把 jar 放到 {_CANDIDATES[0]}，或设环境变量 MYSQL_JDBC_JAR 指到它。")
    return b.getOrCreate()


# ---------------- 类别规则（沿用你之前验证过的关键词） ----------------
def tag_cluster(col):
    return (
        F.when(col.rlike("编写|写一个|写一段|写个|设计一个|程序|函数|Python|算法|代码"), "代码生成")
         .when(col.rlike("翻译|译成|翻成|将.*翻|翻译成"), "翻译")
         .when(col.rlike("重写|改写|修改|润色"), "改写")
         .when(col.rlike("提取|抽取|从中.*(找|选)|从.*中"), "抽取")
         .when(col.rlike("根据|给定|针对|对于.*(文本|文章|段落|句子|信息|材料|内容|数据)"), "基于文本")
         .when(col.rlike("回答|问答|提问|问题"), "问答")
         .otherwise("其他")
    )


def main():
    args = p.parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()
    print("=" * 60)
    print("[1/6] 读原始数据:", args.input)
    df = spark.read.json(args.input)
    n_raw = df.count()
    print(f"      原始条数: {n_raw:,}")

    print(f"[2/6] 清边界: output 长度 >= {args.min_len}")
    df_clean = df.filter(F.length("output") >= args.min_len)
    n_clean = df_clean.count()
    print(f"      清边界后: {n_clean:,}   （剔掉 {n_raw - n_clean:,} 条）")

    print("[3/6] 按 output 去重")
    df_dedup = df_clean.dropDuplicates(["output"])
    n_dedup = df_dedup.count()
    print(f"      去重后:   {n_dedup:,}   （剔掉 {n_clean - n_dedup:,} 条）")

    print("[4/6] 打类别标签 + 派生维度列")
    df_bi = (df_dedup
             .withColumn("cluster", tag_cluster(F.col("instruction")))
             .withColumn("output_len", F.length("output"))
             .withColumn("length_bucket",
                         F.when(F.col("output_len") < 50, "1_短(<50)")
                          .when(F.col("output_len") < 200, "2_中短(50-200)")
                          .when(F.col("output_len") < 500, "3_中(200-500)")
                          .when(F.col("output_len") < 2000, "4_长(500-2000)")
                          .otherwise("5_超长(>=2000)"))
             .withColumn("has_code", F.col("output").rlike("```").cast("int"))
             .withColumn("has_url",  F.col("output").rlike("http").cast("int"))
             .withColumn("has_img",  F.col("output").rlike(r"!\[").cast("int")))

    if args.dry_run:
        print("[5/6] dry-run：只输出统计，不写库")
        print("\n--- 六类分布 ---")
        total = n_dedup
        rows = (df_bi.groupBy("cluster").count()
                .orderBy(F.desc("count")).collect())
        for r in rows:
            print(f"  {r['cluster']:<8} {r['count']:>10,}   {r['count']/total*100:5.1f}%")
        print("\n--- 长度分桶 ---")
        for r in (df_bi.groupBy("length_bucket").count()
                  .orderBy("length_bucket").collect()):
            print(f"  {r['length_bucket']:<16} {r['count']:>10,}   {r['count']/total*100:5.1f}%")
        print("\n--- 长度分位数 ---")
        q = df_bi.select("output_len").approxQuantile("output_len",
                                                      [0.5, 0.9, 0.95, 0.99], 0.01)
        print(f"  P50={q[0]:.0f}  P90={q[1]:.0f}  P95={q[2]:.0f}  P99={q[3]:.0f}")
        print("\n--- 含代码 / 链接 / 图片标记 ---")
        df_bi.agg(F.sum("has_code").alias("含代码块"),
                  F.sum("has_url").alias("含链接"),
                  F.sum("has_img").alias("含图片标记")).show()
        print("\n--- instruction 前 6 字 Top 10（看模板化） ---")
        (df_bi.select(F.substring("instruction", 1, 6).alias("prefix"))
              .groupBy("prefix").count().orderBy(F.desc("count"))
              .show(10, truncate=False))
        print("\n--- 数据样例 ---")
        df_bi.select("cluster", "output_len", "length_bucket",
                     F.substring("instruction", 1, 40).alias("instruction")).show(5, truncate=50)
        print(f"\n耗时 {time.time()-t0:.1f}s。确认没问题后去掉 --dry-run 写库。")
        spark.stop()
        return

    n_part = 64
    spark.conf.set("parquet.enable.dictionary", "false")  # 关掉文本列字典编码
    df_bi.repartition(n_part).write.mode("overwrite").parquet(args.out)
    print(f"[5/6] 写 parquet 备份: {args.out}")
    (df_bi.write.mode("overwrite").parquet(args.out))

    to_write = df_bi.select("cluster", "instruction", "output",
                            "output_len", "length_bucket",
                            "has_code", "has_url", "has_img")
    n_write = n_dedup
    if args.sample and args.sample > 0 and args.sample < n_dedup:
        frac = args.sample / n_dedup
        to_write = to_write.sample(fraction=frac, seed=42)
        n_write = to_write.count()
        print(f"      采样写入: 目标 {args.sample:,}，实际 {n_write:,} 条")

    to_write.repartition(n_part).write.mode("overwrite").parquet(args.out)
    print(f"[6/6] 写 MySQL: bigdata.{args.table}  （{n_write:,} 条，JDBC 单线程，耐心等）")
    print(f"      用户: {MYSQL_USER}   驱动: {JDBC_JAR}")
    (to_write.write
     .mode("overwrite")
     .format("jdbc")
     .option("url", MYSQL_URL)
     .option("dbtable", args.table)
     .option("user", MYSQL_USER)
     .option("password", MYSQL_PWD)
     .option("driver", "com.mysql.cj.jdbc.Driver")
     .option("batchsize", 10000)
     .option("truncate", "true")
     .save())

    print(f"完成，耗时 {time.time()-t0:.1f}s")
    print("\n下一步：打开 Metabase → 添加数据库 bigdata → 找到表", args.table)
    spark.stop()


if __name__ == "__main__":
    main()
