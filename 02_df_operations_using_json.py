from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Netflix analysis").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.load("emp.json", format="json")
df.coalesce(1).write.mode("overwrite").parquet("Employees")
df.show()

java = df.filter(df.stream == "JAVA")
java.show()

java.coalesce(1).write.mode("overwrite").parquet("JavaEmployees")