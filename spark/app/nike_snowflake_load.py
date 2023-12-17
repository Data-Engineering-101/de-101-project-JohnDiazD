import pandas
import os
from common import utils
from snowflake.connector import connect, SnowflakeConnection
spark = utils.get_spark_session()

outPath = "/opt/bitnami/spark/workspace/extract/out"


def upload_to_snowflake(connection: SnowflakeConnection, data_frame, table_name):
  with connection.cursor() as cursor:
    column_secrets = ['%s'] * len(data_frame.columns)
    column_preparated_str = ','.join(column_secrets)
    query = f"INSERT INTO {table_name}  VALUES ({column_preparated_str})"
    cursor.executemany(query, data_frame.values.tolist())


with connect(
    account="EBB34901",
    user="jdiaz2",
    password="Capgemini12",
    database="NIKE",
    schema="PUBLIC",
    warehouse="COMPUTE_WH"
) as connection:
  list_dir = os.listdir(f"{outPath}/category")
  for file_name in list_dir:
    df = pandas.read_csv(f'{outPath}/category/{file_name}')
    upload_to_snowflake(connection, df, "category")

  list_dir = os.listdir(f"{outPath}/products")
  for file_name in list_dir:
    df = pandas.read_csv(f'{outPath}/products/{file_name}')
    upload_to_snowflake(connection, df, "product")

  list_dir = os.listdir(f"{outPath}/sales")
  for file_name in list_dir:
    df = pandas.read_csv(f'{outPath}/sales/{file_name}')
    upload_to_snowflake(connection, df, "sale")

"""

Spark-swowFlake integration
def loadCategory(): 
    dir_path = f"{outPath}/category"
    list_dir = os.listdir(dir_path)
    for file_name in list_dir:        
      df = spark.read.csv(f'{dir_path}/{file_name}', header=True, inferSchema=True)
      utils.df_write(df, "CATEGORY")


def loadProducts(): 
    dir_path = "out/products"
    list_dir = os.listdir(dir_path)
    for file_name in list_dir:  
      df = spark.read.csv(f'{dir_path}/{file_name}', header=True, inferSchema=True)
      utils.df_write(df, "PRODUCT")

def loadSales(): 
    dir_path = "out/sales"
    list_dir = os.listdir(dir_path)
    for file_name in list_dir:  
      df = spark.read.csv(f'{dir_path}/{file_name}', header=True, inferSchema=True)
      utils.df_write(df, "SALE")
"""