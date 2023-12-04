from pyspark.sql import SparkSession
from pyspark.sql.functions import col,dayofmonth
# Initialize SparkSession
spark = SparkSession.builder.appName("KafkaStreamExample").getOrCreate()

# Read data from Kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "worker2:9092") \
  .option("subscribe", "Demo") \
  .option("startingOffsets", "earliest") \
  .load()

# Convert the value column from Kafka to a string
df = df.selectExpr("CAST(value AS STRING)")

print(df)
# Define a schema for the data (name, age, gender)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField('systime', DateType(),True)
])

# Deserialize the JSON data
df = df.selectExpr("CAST(value AS STRING)").selectExpr("from_json(value, 'name STRING, age INT, gender STRING,systime Date') as data").select("data.*")

# Add 5 years to the age
df = df.withColumn("age", col("age") + 5)


#df = spark.createDataFrame(df.rdd,schema=schema)
#df = df.withColumn('sysdate',dayofmonth('day'))
from pyspark.sql.functions import date_format

# Assuming 'systime' is a valid column in your DataFrame

df = df.withColumn('day_of_week', date_format('systime', 'EEEE'))
query = df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .partitionBy("day_of_week") \
    .option("checkpointLocation", "/user/spark/applicationHistory58796") \
    .start("hdfs://worker1:8020/user/hive/TestPartitionData")

query.awaitTermination()
#df.write.partitionBy("day_of_week").parquet("hdfs://worker1:8020/user/hive/TestPartitionData")
# Write Processed Data to HDFS
#query = df \
#   .writeStream \
#    .format("Parquet") \
#    .option("path", "/user/hive/NewDataTest") \
#    .option("checkpointLocation", "/user/spark/applicationHistory5796") \
#    .start()

#query.awaitTermination()
#spark.stop()

