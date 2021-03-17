from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Netflix analysis").config("spark.some.config.option", "some-value").getOrCreate()

Student = Row("ID", "Name", "Age", "Area of Interest")
s1 = Student("1", 'Jack', 22, "Data Science")
s2 = Student("2", 'Luke', 21, "Data Analytics")
s3 = Student("3", 'Leo', 24, "Micro Services")
s4 = Student("4", 'Mark', 21, "Data Analytics")
StudentData=[s1,s2,s3,s4]
df=spark.createDataFrame(StudentData)
age = df.describe('Age')

nameSorted = df.select("ID", "Name", "Age").orderBy("Name",ascending=False)
df.show()
age.show()
nameSorted.show()

age.coalesce(1).write.mode("overwrite").parquet("Age")
nameSorted.coalesce(1).write.mode("overwrite").parquet("NameSorted")