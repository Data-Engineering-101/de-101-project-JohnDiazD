import pandas
import os
from common import utils
spark = utils.get_spark_session()


def loadCategory(): 
    dir_path = "out/category"
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

      
def load():
   loadCategory();
   loadProducts();
   loadSales();

load()