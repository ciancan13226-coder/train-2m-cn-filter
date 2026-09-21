import os
os.environ["HADOOP_HOME"] = r"E:\hadoop"
os.environ["hadoop.home.dir"] = r"E:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";E:\hadoop\bin"
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("MiniTest").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions","2").\
        config("spark.sql.warehouse.dir","./mini_warehouse").\
        getOrCreate()

    df = spark.read.json("../input/train_2M_CN.json")
    print("总数:", df.count())

    # ===== 第 3 步：清边界 =====
    # 一条同时干掉空值和过短（空值长度=0，自然 <10）
    df_clean = df.filter(F.length("output") >= 10)
    print("清边界后:", df_clean.count())

    # ===== 第 4 步：去重 =====
    # 按 output 去重，保留每组一条
    df_dedup = df_clean.dropDuplicates(["output"])
    print("去重后:", df_dedup.count())

    # ===== 第 5 步：写出 JSONL =====
    df_dedup.select("instruction", "output") \
        .write.mode("overwrite") \
        .json("./output/sft_dataset")

    print("写出完成")


    # 1. 完全重复的 output 有多少
    dup_output = df.groupBy("output").count().filter("count > 1")
    print("重复的 output 组数:", dup_output.count())

    # 2. 整行完全重复
    print("整行去重后:", df.dropDuplicates().count())

    # 3. 长度分布
    df.select(F.length("output").alias("len")).describe().show()

    # # 4. 指令的开头几个字——看模板化程度
    # df.select(F.substring("instruction", 1, 6).alias("prefix")) \
    #     .groupBy("prefix").count() \
    #     .orderBy(F.desc("count")).show(200, truncate=False)
    #
    # print("数学|方程|计算",df.filter("instruction rlike '数学|方程|计算'").count())
    # print("翻译|英文",df.filter("instruction rlike '翻译|英文'").count())
    # print("将以下",df.filter("instruction rlike '将以下'").count())
    #
    # print("数学|方程|计算",df.filter("instruction rlike '数学|方程|求解'").count())
    #
    # print("翻译|英文",df.filter("instruction rlike '将以下' and instruction rlike '翻译|英文'").count())

    # def tag_cluster(col):
    #     return (
    #         F.when(col.rlike("编写|写一个|写一段|设计一个|程序|函数|Python|算法|代码"), "代码生成")
    #         .when(col.rlike("翻译|译成|翻成|将.*翻|翻译成"), "翻译")
    #         .when(col.rlike("重写|改写|修改|润色"), "改写")
    #         .when(col.rlike("提取|抽取|从中.*(找|选)|从.*中"), "抽取")
    #         .when(col.rlike("根据|给定|针对|对于.*(文本|文章|段落|句子|信息|材料|内容|数据)"), "基于文本")
    #         .when(col.rlike("回答|问答|提问|问题"), "问答")
    #         .otherwise("其他")
    #     )
    #
    #
    # df_labeled = df.withColumn("cluster", tag_cluster(F.col("instruction")))
    #
    # # 第一段：全量计数（要准确数字，扫一次）
    # df_labeled.groupBy("cluster").count().orderBy(F.desc("count")).show()
    #
    # # 第二段：用 1% 采样做验证（不碰全量）
    # df_small = df_labeled.sample(fraction=0.01, seed=42)
    # df_small.cache()          # 2 万条，缓存毫无压力
    #
    # for c in ["代码生成", "翻译", "改写", "抽取", "基于文本", "问答", "其他"]:
    #     print(f"===== {c} =====")
    #     df_small.filter(F.col("cluster") == c) \
    #         .select("instruction").show(3, truncate=0)   # ← truncate=0，不截断
    #
    # df_labeled.filter(F.col("cluster") == "其他") \
    #     .select(F.substring("instruction", 1, 6).alias("prefix")) \
    #     .groupBy("prefix").count() \
    #     .orderBy(F.desc("count")).show(30, truncate=False)

    # ...

    #
    # # 1. 空输出和过短输出,到底多少条
    # count_kong = df.filter(F.length("output") == 0).count()
    # print(count_kong)
    #
    # count_guoduan = df.filter(F.length("output") < 10).count()
    # print(count_guoduan)
    #
    # # 2. 重复 output 实际涉及多少条(不是多少组)
    # df.groupBy("output").count().filter("count > 1") \
    #     .select(F.sum("count")).show()
    #
    # # 3. 长度分位数(比 describe 有用得多)
    # df_fenweishu = df.select(F.length("output").alias("len")) \
    #     .approxQuantile("len", [0.5, 0.9, 0.95, 0.99], 0.01)
    # print(df_fenweishu)
    #
    # # 5. 验证长度截断
    # df_len = df.withColumn("len", F.length("output"))
    #
    # print("len == 13535:", df_len.filter("len = 13535").count())
    # print("len > 2000:", df_len.filter("len > 2000").count())
    # print("len > 5000:", df_len.filter("len > 5000").count())
    #
    # # 看几个"超长样本"的末尾，是不是断在半句
    # df_len.filter("len > 10000") \
    #     .select(F.substring("output", -50, 50).alias("末尾50字")) \
    #     .show(5, truncate=False)
    #
    # for t in [800, 1000, 1200, 1500]:
    #     print(f"len > {t}:", df_len.filter(f"len > {t}").count())

