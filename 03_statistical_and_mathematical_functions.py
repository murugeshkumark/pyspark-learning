from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Netflix analysis").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.range(0, 10).withColumn('rand1', rand(seed=10)).withColumn('rand2', rand(seed=27))
df.show()
cov = df.stat.cov('rand1', 'rand2')
corr = df.stat.corr('rand1', 'rand2')

Stats = Row("Stats", "Value")
s1 = Stats('Co-variance',cov)
s2 = Stats('Correlation',corr)
StatsData=[s1,s2]
df=spark.createDataFrame(StatsData)
df.show()

df.coalesce(1).write.mode("overwrite").parquet("Result")
