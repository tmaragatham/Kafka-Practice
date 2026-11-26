from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder.appName("Stock-Market-Tick-Alerts")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("change_percent", DoubleType()),
    StructField("event_time", StringType()),
])

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "stock-ticks")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Example alert conditions: large percentage movement or unusually high volume
alerts_df = parsed_df.filter(
    (col("change_percent") >= 2.0)
    | (col("change_percent") <= -5.0)
    | (col("volume") >= 5000)
)

query = (
    alerts_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/chk_stock_alerts")
    .start()
)

print(
    "Spark Structured Streaming listening for stock alerts "
    "(change >= +5%, change <= -5%, or volume >= 9000)... "
    "Press Ctrl+C to stop."
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming query...")
    query.stop()
finally:
    spark.stop()
