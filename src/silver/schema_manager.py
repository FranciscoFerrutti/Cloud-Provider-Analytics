"""
Schema Manager for Silver Layer
Handles schema evolution and versioning normalization
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    """
    Manages schema standardization and version compatibility.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def standardize_schema(self, df: DataFrame, entity_name: str) -> DataFrame:
        """
        Standardize schema for any entity based on known canonical fields.
        
        Args:
            df: Input DataFrame with potentially mixed schema versions
            entity_name: Name of the entity (e.g., 'usage_events')
            
        Returns:
            DataFrame with canonical schema
        """
        logger.info(f"Standardizing schema for {entity_name}")
        
        if entity_name == "usage_events":
            return self._standardize_usage_events(df)
        
        # Add other entities if specific versioning logic is needed
        return df

    def _standardize_usage_events(self, df: DataFrame) -> DataFrame:
        """
        Standardize usage events to handle schema version 1 and 2
        
        Version 1: timestamp, no genai/carbon
        Version 2: event_ts, carbon_kg, genai_tokens
        """
        
        # 1. Handle timestamp renaming (v1 -> v2)
        if "timestamp" in df.columns and "event_ts" not in df.columns:
            logger.info("Detecting v1 schema: mapping 'timestamp' to 'event_ts'")
            df = df.withColumnRenamed("timestamp", "event_ts")
            
        # 2. Ensure Schema Version 2 columns exist (Backfill for v1 data)
        required_cols = {
            "carbon_kg": (DoubleType(), 0.0),
            "genai_tokens": (LongType(), 0),
            "schema_version": (StringType(), "1.0") # Default to 1.0 if missing
        }
        
        for col_name, (dtype, default_val) in required_cols.items():
            if col_name not in df.columns:
                logger.info(f"Column '{col_name}' missing, adding with default: {default_val}")
                df = df.withColumn(col_name, lit(default_val).cast(dtype))
            else:
                # Ensure correct type even if exists (e.g. read as string)
                df = df.withColumn(col_name, col(col_name).cast(dtype))
        
        # 3. Canonical Column Order / Type Casting
        # We enforce types for critical columns
        df = df.withColumn("event_ts", col("event_ts").cast(TimestampType())) \
               .withColumn("cost_usd_increment", col("cost_usd_increment").cast(DoubleType())) \
               .withColumn("value", col("value").cast(DoubleType()))
               
        return df
