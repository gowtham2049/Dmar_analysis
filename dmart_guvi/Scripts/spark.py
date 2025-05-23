from pyspark.sql import SparkSession

def spark_creator():
    spark=SparkSession \
    .builder \
    .appName("Dmart_analysis") \
    .master("local[4]") \
    .getOrCreate()
    return spark