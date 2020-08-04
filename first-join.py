from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("First Join Practical").getOrCreate()
region_data = spark.createDataFrame([("Family with grown ups","PN"), \
                            ("Driven Growers","GJ"), \
                            ("Conservative families","DD"), \
                            ("Cruising Seniors","DL"), \
                            ("Avarage Family","MN"), \
                            ("Living Well","KA"), \
                            ("Successful hedonists","JH"), \
                            ("Retired and Religious","AX"), \
                            ("Career Loners","HY"), \
                            ("Farmers","JH")], \
                            schema = StructType().add("Customer_main_type","string").add("Region Code","string"))
region_data.show()
df = spark.read.csv("customer_data.csv",inferSchema = True, header = True)
new_df = df.join(region_data,on="Customer_main_type")
#new_df.show()
new_df.groupBy("Region Code").count().show()
