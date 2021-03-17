from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as T
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,DateType

spark = SparkSession \
    .builder \
    .appName("Netflix analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path = "Employees"
df = spark.read.parquet(path)

def test_data_frame_Creation():
    if df!=None:
        assert df.count() == 5

def test_data_frame_content_path1():
    if df!=None:
        assert df.collect()[0][1] == "Mathew"
        assert df.collect()[0][0] == "22"
        assert df.collect()[0][2] == "JAVA"
    
path="JavaEmployees"
df1 = spark.read.parquet(path)

def test_data_frame_length():
    if df1!=None:
        assert df1.count() == 3

def test_data_frame_content_path2():
    if df1!=None:
        for i in range(0,3):
            assert df1.collect()[i][2] == "JAVA"

