from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType

# Define Spark session
spark = SparkSession.builder \
    .appName("WeatherConsumer") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .getOrCreate()
    #.config("spark.jars.packages",
     #   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
      #  "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.4"
       # "net.snowflake:snowflake-jdbc:3.13.30") \ 

    

# Define schema for weather data
schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", FloatType()) \
    .add("humidity", IntegerType()) \
    .add("description", StringType()) \
    .add("timestamp", StringType())

# Snowflake connection options
sfOptions = {
    "sfURL": "QQWBYUC-QCB49551.snowflakecomputing.com",  # Ex: xy12345.us-east-1
    "sfUser": "snowpravi",
    "sfPassword": "Pravalika@1812",
    "sfDatabase": "WEATHER_DB",
    "sfSchema": "WEATHER_SCHEMA",
    "sfWarehouse": "WEATHER_WH",
    "sfRole": "ACCOUNTADMIN"  # Adjust based on your user role
}

print("Starting the consumer...")

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "latest") \
    .load()
print("Kafka stream loaded")


# Parse JSON messages
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write streaming data to Snowflake using foreachBatch
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()


# Write stream to Snowflake
'''
query = json_df.writeStream \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "WEATHER_DATA") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/snowflake_checkpoint") \
    .start()

query.awaitTermination()
'''

