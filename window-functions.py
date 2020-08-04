from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Window Functions").getOrCreate()
df = spark.read.csv("customer_data.csv",inferSchema = True, header = True)
#win = Window.orderBy(df["Avg_salary"].desc())
#df1 = df.withColumn("rank",row_number().over(win).alias("rank"))
#df1.show()
win1 = Window.partitionBy("Customer_subtype").orderBy(df['Avg_salary'].desc())
df2 = df.withColumn("rank",row_number().over(win1).alias("rank"))
#df2.groupBy("rank").count().orderBy("rank").show()
df2.filter(col("rank")<4).show()
