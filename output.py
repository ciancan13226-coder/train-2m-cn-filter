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

    df_out = spark.read.json("./output/sft_dataset")
    print("读回来的条数:", df_out.count())  # 应该是 1,863,135
    print("列名:", df_out.columns)
    df_out.show(2, truncate=50)


    print("Markdown 图片:", df_out.filter(F.col("output").rlike(r"!\[")).count())
    print("含 URL:", df_out.filter(F.col("output").rlike("http")).count())
