from pyspark.sql import SparkSession, Row

year = 2022
month = 1

# SparkSession
spark = SparkSession.builder \
    .appName("w5 m2") \
    .getOrCreate()

# reading parquet
df = spark.read.parquet("hdfs://namenode:9000/20221.parquet")

# DataFrame to RDD
rdd = df.rdd

# filtering
filtered_rdd = rdd.filter(lambda row: (row.fare_amount > 0) and (row.tpep_pickup_datetime.year == year) and (row.tpep_pickup_datetime.month == month))

# Trips count
total_trips = filtered_rdd.count()

# total revenue, total distance
total_revenue, total_distance = filtered_rdd.map(lambda row: (row.total_amount, row.trip_distance)) \
    .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg_distance = total_distance / total_trips

# rdd by date
date_rdd = filtered_rdd.map(lambda row: (row.tpep_pickup_datetime.date(), (1, row.total_amount))) \
    .reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))

c1 = date_rdd.collect()

# print
print(f"Total trips: {total_trips}")
print(f"Total revenue: {total_revenue}")
print(f"Avg distance: {avg_distance}")

for key, value in c1:
    print(key, value[0], value[1])

output_data = [
    ("Total trips", total_trips),
    ("Total revenue", total_revenue),
    ("Avg distance", avg_distance)
]

total_rdd = spark.sparkContext.parallelize(output_data)
total_df = total_rdd.toDF(["Metric", "Value"])
total_df.write.option("header", "true").csv("hdfs://namenode:9000/output_stats.csv")

date_df = spark.createDataFrame(date_rdd.map(lambda x: Row(date=x[0], count=x[1][0], revenue=x[1][1])))
date_df.write.option("header", "true").csv("hdfs://namenode:9000/output_by_date.csv")

# Stop Spark session
spark.stop()
