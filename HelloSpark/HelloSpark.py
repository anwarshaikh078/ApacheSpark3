import sys
from pyspark.sql import *
import configparser

from pyspark import SparkConf


def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()



    if len(sys.argv) != 2:
        print("Usage: HelloSpark ")
        sys.exit(-1)

    print("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sys.argv[1])
    #print(survey_raw_df)
    partitioned_survey_df = survey_raw_df.repartition(2)
    #print(partitioned_survey_df)
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    print("Finished HelloSpark")
    spark.stop()
