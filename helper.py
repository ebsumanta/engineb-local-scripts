
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pandas as pd

spark = SparkSession.builder.appName('helper').getOrCreate()


df = spark.read.option('header','true').csv("./d2/dbfs/cdm/glDetail.csv")

#df.show(truncate=False)

df = df.filter("amount  > 1000")
df = df.filter("journalIdLineNumber == 1")
df.show(vertical = True)
pdf = df.toPandas()
pdf.to_csv(f"helper.csv",index=False)

# df.show(truncate=False)

