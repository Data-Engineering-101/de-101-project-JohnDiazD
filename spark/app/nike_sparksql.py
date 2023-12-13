from common import utils
import os

spark = utils.get_spark_session()

def _transformProductData():
    #product_path = f"{path}/data/products/{file}"
    product_path = "/opt/bitnami/spark/workspace/extract/data/products/nike_21NOV2023_2125.csv"
    print(product_path)
    spark.read.csv(product_path, header=True, inferSchema=True).createOrReplaceTempView("product")
    product_df = spark.sql('select UID,title,subtitle,category,productID product_id from product').toPandas()
    product_df.to_csv("/opt/bitnami/spark/workspace/extract/out/products/products.csv", index=False)

    category_df = spark.sql('select distinct category from product').toPandas()
    category_df['category'] = category_df.apply(lambda row: row['category'] if row["category"] else 'None', axis=1)
    category_df.to_csv(f"/opt/bitnami/spark/workspace/extract/out/category/category.csv", index=False)


_transformProductData()
spark.stop()

"""
def _transformCategoryData(path,file):
    spark.read.csv(f"{path}/data/products/{file}", header=True, inferSchema=True).createOrReplaceTempView("product")
    category_df = spark.sql('select distinct category from product').toPandas()
    #category_df['category_key'] = category_df.apply(lambda row: '', axis=1)
    category_df['category'] = category_df.apply(lambda row: row['category'] if row["category"] else 'None', axis=1)
    category_df.to_csv(f"{path}/out/category/{file}", index=False)

def _transformSalesData(path, year, month, day, file):
    spark.read.csv(f"{path}/data/sales/{year}/{month}/{day}/{file}", header=True, inferSchema=True).createOrReplaceTempView("sales")
    sales_df = spark.sql("select UID,currency,sales,quantity,split(date, '-')[0] as year, split(date, '-')[1] month  from sales").toPandas()
    sales_df.to_csv(f"{path}/out/sales/{file}", index=False)

def transform():
    
    cwd = os.getcwd()
    print(cwd)
    path = "/opt/spark/app/extract"
    product_dir = os.listdir(f"{path}/data/products")
    for file_name in product_dir:
        print(file_name)
        _transformProductData(path,file_name)
        _transformCategoryData(path,file_name)
    
    sales = "/extract/data/sales"
    year_dir = os.listdir(sales)
    for year in year_dir:
        sales_year = f"{sales}/{year}"
        year_dir = os.listdir(sales_year)
        for month in year_dir:
            sales_month = f"{sales_year}/{month}"
            month_dir = os.listdir(sales_month)
            for day in month_dir:
                sales_day = f"{sales_year}/{month}/{day}"
                sales = os.listdir(sales_day)
                for sale in sales:
                    _transformSalesData(path,year,month,day,sale)


transform()
"""