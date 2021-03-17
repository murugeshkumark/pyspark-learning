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

path1 = "Age"
df1 = spark.read.parquet(path1)

def test_data_frame_Creation():
    if df1!=None:
        assert df1.count() == 5

def test_data_frame_content():
    if df1 != None:
        collection = df1.collect()
        assert collection[0][0] == "count"
        assert collection[0][1] == "4"
        assert collection[2][0] == "stddev"
        assert collection[2][1] == "1.414213562373095"
        assert collection[4][0] == "max"
        assert collection[4][1] == "24"
        
path2 = "NameSorted"
df2 =  spark.read.parquet(path2)
def test_data_frame_content_for_sorted():
    if df2!=None:
        collection = df2.collect()
        assert collection[0][0] == "4"
        assert collection[0][1] == "Mark"
        assert collection[1][1] == "Luke"
        assert collection[2][1] == "Leo"
        assert collection[3][1] == "Jack"
