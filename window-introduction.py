'''
At its core, a window function calculates a return value for every input row of a table based on a group of rows, called the Frame. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way.

Window functions allow users of Spark SQL to calculate results such as the rank of a given row or a moving average over a range of input rows.
'''
#Importing necessry functions
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number,dense_rank,max
#Creating Spark SQL session and reading from a CSV File
spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Window function Introduction").getOrCreate()
df = spark.read.csv("product_revenue.csv", inferSchema = True, header = True)
#Answering the question: "What are the best-selling and the second best-selling products in every category?"
win_func = Window.partitionBy("category").orderBy(df["revenue"].desc())
df_first = df.withColumn("rank",dense_rank().over(win_func))
df_first.where(col("rank") <= 2).orderBy(col("rank").asc()).select("product","category","revenue").show()
#Answering the question: "What is the difference between the revenue of each product and the revenue of the best selling product in the same category as that product?"
win_func_max = Window.partitionBy("category").orderBy(df["revenue"].desc())
df_max = df.withColumn("revenue_diff",max(df["revenue"]).over(win_func_max)-df["revenue"])
df_max.show()
