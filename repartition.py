from pyspark.sql.functions import *

readPath = 's3://bucket/input/'
whritePath = 's3://bucket/output/'

df = spark.read.parquet(readPath)

(
    df
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .repartition("year", "month", "day")
    .write
    .format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("year", "month", "day")
    .save(whritePath)
)
