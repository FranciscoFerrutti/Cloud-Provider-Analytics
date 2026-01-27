import os
import sys
from pyspark.sql import SparkSession

print(f"Python version: {sys.version}")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

try:
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    print("SparkSession created successfully!")
    spark.stop()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)
