from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Spark Join Introduction").getOrCreate()
schema1 = StructType().add("key","string").add("val11","double").add("val12","double")
schema2 = StructType().add("key","string").add("val21","double").add("val22","double")
df1 = spark.createDataFrame([("abc",1.1,1.2),("def",3.0,3.4)], schema = schema1)
df2 = spark.createDataFrame([("abc",2.1,2.2),("xyz",3.1,3.2)], schema = schema2)

'''
Pyspark provides conditions which lets us join two dataframes insted of the "on" parameter.
'''
print("inner join with condition: df1.key == df2.key ....")
df_cond = df1.join(df2, df1.key == df2.key, how = "inner")
df_cond.show()
print("Inner Join with condition: df1.key > df2.key ....")
df_cond = df1.join(df2, df1.key > df2.key, how = "inner")
df_cond.show()
print("Inner Join with Multiple conditions: df1.val11 < df2.val21 and df1.val12 < df2.val22")
df_cond = df1.join(df2, [df1.val11 < df2.val21, df1.val12 < df2.val22], how = "inner")
df_cond.show()
print("Inner Join with Multiple conditions: df1.val11 > df2.val21 or df1.val12 < df2.val22")
df_cond = df1.join(df2, [(df1.val11 > df2.val21) | (df1.val12 < df2.val22)], how = "inner")
df_cond.show()
