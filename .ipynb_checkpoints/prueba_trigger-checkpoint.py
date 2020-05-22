from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("prueba") \
    .getOrCreate()

df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 90000) \
    .option("ramUpTime", 1) \
    .load()

rate_raw_data = df.selectExpr("CAST(timestamp as string)", "CAST(value as string)")

stream_query = rate_raw_data.writeStream \
    .format("console") \
    .queryName("Default") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

once_query = rate_raw_data.writeStream \
    .format("console") \
    .queryName("Once") \
    .trigger(once=True) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
    
processing_time_query = rateRawData.writeStream \
    .format("console") \
    .queryName("Micro Batch") \
    .trigger(processingTime=20) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

stream_query.awaitTermination()
