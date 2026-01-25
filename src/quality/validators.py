"""
Data quality validation rules and functions
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, row_number
import logging

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Data quality validation for Bronze and Silver layers"""
    
    def __init__(self, quarantine_path: str):
        """
        Initialize validator
        
        Args:
            quarantine_path: Path to store invalid records
        """
        self.quarantine_path = quarantine_path
    
    def validate_usage_events(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validate usage events according to quality rules
        
        Rules:
        - event_id not null and unique per window
        - cost_usd_increment >= -0.01
        - unit not null if value is not null
        - schema_version compatibility (v1/v2)
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Validating usage events")
        
        # Create validation flags
        validation_df = df.withColumn(
            "is_valid",
            when(
                col("event_id").isNull(), False
            ).when(
                col("cost_usd_increment") < -0.01, False
            ).when(
                (col("value").isNotNull()) & (col("unit").isNull()), False
            ).when(
                col("event_ts").isNull(), False
            ).when(
                col("org_id").isNull(), False
            ).otherwise(True)
        )
        
        # Separate valid and invalid records
        valid_df = validation_df.filter(col("is_valid") == True).drop("is_valid")
        invalid_df = validation_df.filter(col("is_valid") == False).drop("is_valid")
        
        # Add validation error reason
        invalid_df = invalid_df.withColumn(
            "validation_error",
            when(col("event_id").isNull(), "event_id_null")
            .when(col("cost_usd_increment") < -0.01, "cost_below_threshold")
            .when((col("value").isNotNull()) & (col("unit").isNull()), "unit_missing")
            .when(col("event_ts").isNull(), "event_ts_null")
            .when(col("org_id").isNull(), "org_id_null")
            .otherwise("unknown")
        )
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid records")
        
        return valid_df, invalid_df
    
    def validate_schema_version(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validate and handle schema version compatibility
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (compatible_df, incompatible_df)
        """
        logger.info("Validating schema versions")
        
        # Check for required fields based on schema version
        compatible_df = df.filter(
            (col("schema_version").isNull()) |
            (col("schema_version") == 1) |
            (col("schema_version") == 2)
        )
        
        incompatible_df = df.filter(
            (col("schema_version").isNotNull()) &
            (col("schema_version") != 1) &
            (col("schema_version") != 2)
        )
        
        if incompatible_df.count() > 0:
            incompatible_df = incompatible_df.withColumn(
                "validation_error",
                lit("incompatible_schema_version")
            )
        
        return compatible_df, incompatible_df
    
    def deduplicate_by_event_id(self, df: DataFrame, window_col: str = "event_ts") -> DataFrame:
        """
        Deduplicate events by event_id within a time window
        
        Args:
            df: Input DataFrame
            window_col: Column to use for windowing
            
        Returns:
            Deduplicated DataFrame
        """
        logger.info("Deduplicating by event_id")
        
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("event_id").orderBy(col(window_col).desc())
        
        deduplicated_df = df.withColumn(
            "row_num",
            row_number().over(window_spec)
        ).filter(col("row_num") == 1).drop("row_num")
        
        return deduplicated_df
    
    def save_quarantine(self, invalid_df: DataFrame, layer: str, table_name: str):
        """
        Save invalid records to quarantine
        
        Args:
            invalid_df: DataFrame with invalid records
            layer: Data layer (bronze, silver)
            table_name: Table name
        """
        if invalid_df.count() > 0:
            quarantine_path = f"{self.quarantine_path}/{layer}/{table_name}"
            logger.warning(f"Saving {invalid_df.count()} invalid records to {quarantine_path}")
            
            invalid_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .parquet(quarantine_path)
        else:
            logger.info("No invalid records to quarantine")

