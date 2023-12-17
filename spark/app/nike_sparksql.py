from common import utils
import os

spark = utils.get_spark_session()

dataPath = "/opt/bitnami/spark/workspace/extract/data"

outPath = "/opt/bitnami/spark/workspace/extract/out"

def _transformProductData():
    product_dir = os.listdir(f"{dataPath}/products")
    for file_name in product_dir:    
        product_path = f"{dataPath}/products/{file_name}"

        spark.read.csv(product_path, header=True, inferSchema=True).createOrReplaceTempView("product")
        product_df = spark.sql('select UID,title,subtitle,category,productID product_id' + 
                               ' from product where UID IS NOT NULL and title is NOT NULL and subtitle is NOT NULL and category IS NOT NULL and productID IS NOT NULL').toPandas()
        product_df.to_csv(f"{outPath}/products/products.csv", index=False)

        category_df = spark.sql('select distinct category from product where category IS NOT NULL').toPandas()
        category_df['category'] = category_df.apply(lambda row: row['category'] if row["category"] else 'None', axis=1)
        category_df.to_csv(f"{outPath}/category/categories.csv", index=False)

def _transformDailySalesData(path, year, month, day, file):
    filePath = f"{path}/{year}/{month}/{day}/{file}"
    spark.read.csv(filePath, header=True, inferSchema=True).createOrReplaceTempView("sales")
    sales_df = spark.sql("select UID,currency,sales,quantity,split(date, '-')[0] as year, split(date, '-')[1] month  from sales").toPandas()
    sales_df.to_csv(f"{outPath}/sales/{file}", index=False)


def _transformSalesData():
    path = f"{dataPath}/sales"
    year_dir = os.listdir(path)
    for year in year_dir:
        sales_year = f"{path}/{year}"
        year_dir = os.listdir(sales_year)
        for month in year_dir:
            sales_month = f"{sales_year}/{month}"
            month_dir = os.listdir(sales_month)
            for day in month_dir:
                sales_day = f"{sales_year}/{month}/{day}"
                sales = os.listdir(sales_day)
                for sale in sales:
                    _transformDailySalesData(path,year,month,day,sale)

_transformProductData()
_transformSalesData()
spark.stop()