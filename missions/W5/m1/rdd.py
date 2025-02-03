from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

# SparkSession 생성
spark = SparkSession.builder \
    .appName("PySpark Parquet Example") \
    .getOrCreate()

# Parquet 파일 읽기
df = spark.read.parquet("hdfs://namenode:9000/20221.parquet")


df.show()
