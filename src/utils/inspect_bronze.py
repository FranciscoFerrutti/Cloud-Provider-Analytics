import sys
import os
sys.path.append('/app')
from pyspark.sql import SparkSession
from src.utils.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("InspectBronze").getOrCreate()

bronze_path = Config.get_bronze_path("customers_orgs")
logger.info(f"Reading from {bronze_path}")

try:
    df = spark.read.parquet(bronze_path)
    df.printSchema()
    df.show(5)
    
    # Check specifically for VoidType
    for field in df.schema.fields:
        if str(field.dataType) == "VoidType" or str(field.dataType) == "NullType":
            logger.error(f"Column {field.name} is VoidType/NullType!")
            
except Exception as e:
    logger.error(f"Error reading bronze: {e}")
