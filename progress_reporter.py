import logging
from pyspark.sql import SparkSession


def run_spark_job(spark):
    #TODO read format as Kafka and add various configurations
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:<your port>") \
        .option("subscribe", "<your topic name>") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    agg_df = df.groupby("topic").count()

    # TODO complete this
    # play around with processingTime to see how the progress report changes
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("StructuredStreamingSetup") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
