from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession\
            .builder\
            .master("local[3]")\
            .appName("Partitioning")\
            .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDf = spark.read\
        .format("parquet")\
        .load("dataSource/flight*.parquet")

    logger.info("Num of Partition before: " + str(flightTimeParquetDf.rdd.getNumPartitions()))
    flightTimeParquetDf.groupBy(spark_partition_id()).count().show()

    partitionDf = flightTimeParquetDf.repartition(5)
    logger.info("Num of Partition after: " + str(partitionDf.rdd.getNumPartitions()))
    partitionDf.groupBy(spark_partition_id()).count().show()

    partitionDf.write\
        .format("parquet")\
        .mode("overwrite")\
        .option("path","dataSink/afterPartition/parquet/")\
        .save()

    flightTimeParquetDf.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/afterPartition/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    logger.info("Num of Partition after using PartitonBy: " + str(flightTimeParquetDf.rdd.getNumPartitions()))
    flightTimeParquetDf.groupBy(spark_partition_id()).count().show()
