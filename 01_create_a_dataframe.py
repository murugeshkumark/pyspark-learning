# Put your code here
from pyspark.sql import *
spark = SparkSession.builder.appName("Data Frame Example").config("spark.some.config.option", "some-value").getOrCreate()
Passenger = Row("Name", "age", "source", "destination")
p1 = Passenger('David', 22, "London","Paris")
p2 = Passenger('Steve', 22, "New York","Sydney")
PassengerData=[p1,p2]
df=spark.createDataFrame(PassengerData)
df.show()

# Don't Remove this line 
df.coalesce(1).write.parquet("PassengerData")