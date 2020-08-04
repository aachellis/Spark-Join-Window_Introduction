from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Spark Join Introduction").getOrCreate()
schema1 = StructType().add("key","string").add("val11","double").add("val12","double")
schema2 = StructType().add("key","string").add("val21","double").add("val22","double")
df1 = spark.createDataFrame([("abc",1.1,1.2),("def",3.0,3.4)], schema = schema1)
df2 = spark.createDataFrame([("abc",2.1,2.2),("xyz",3.1,3.2)], schema = schema2)
#df1.show()
#df2.show()
#Inner join
print("Inner join .......")
df_inner = df1.join(df2, on = ["key"], how = "inner")
df_inner.show()
#Outer join
print("Outer join .......")
df_outer = df1.join(df2, on = ["key"], how = "outer")
df_outer.show()
#left join
print("left join .......")
df_left = df1.join(df2, on = ["key"], how = "left")
df_left.show()
#Right Join
print("Right join .......")
df_right = df1.join(df2, on = ["key"], how = "right")
df_right.show()
#Left Semi Join
print("Left Semi join .......")
df_left_semi = df1.join(df2, on = ["key"], how = "left_semi")
df_left_semi.show()
#left Anti Join
print("left Anti join .......")
df_left_anti = df1.join(df2, on = ["key"], how = "left_anti")
df_left_anti.show()
