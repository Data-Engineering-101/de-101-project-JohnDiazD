from pyspark.sql import SparkSession


# Set options below
sfOptions = {
  "sfURL" : "https://mdgzgmc-etb84499.snowflakecomputing.com",
  "sfUser" : "jdiaz2",
  "sfPassword" : "Capgemini12",
  "sfDatabase" : "NIKE",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Spark session builder



def get_spark_session():
  return SparkSession.builder \
    .appName("NikeApp") \
    .getOrCreate()

"""
def get_spark_session():
  return SparkSession.builder \
    .appName("NikeApp") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

"""



"""
# Spark session builder
def get_spark_session():
  return SparkSession.builder \
    .appName("NikeApp") \
    .master("local[*]") \
    .getOrCreate()
"""

    
# Writes a dataframe into the snowflake instance
def df_write(df, table_name):
    df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()