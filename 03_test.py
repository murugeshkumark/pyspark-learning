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

path = "Result"
df = spark.read.parquet(path)

def test_data_frame_Creation():
    if df!=None:
        assert df.count() == 2

def test_data_frame_content_path1():
    if df!=None:
        assert df.collect()[0][0] == "Co-variance"
        assert df.collect()[1][0] == "Correlation"