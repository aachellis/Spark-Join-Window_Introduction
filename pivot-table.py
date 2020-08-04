from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Pivot in Dataframe").getOrCreate()
df = spark.read.csv("customer_data.csv",inferSchema = True, header = True)
df.groupBy("Customer_main_type").pivot("Avg_age").sum("Avg_salary").fillna(0).show()
