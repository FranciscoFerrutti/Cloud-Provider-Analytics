"""
SCD Type 2 Manager for Silver Layer
Handles Slowly Changing Dimensions for Master Data (Orgs, Users, Resources)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_date, when, sha2, concat_ws, md5, coalesce
)
import logging
from typing import List
from src.utils.config import Config

logger = logging.getLogger(__name__)

class SCDManager:
    """
    Manages SCD Type 2 operations for dimension tables.
    Since we are using Parquet (not Delta), we use a full rewrite strategy for dimensions.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def _calculate_hash(self, df: DataFrame, cols: List[str], hash_col_name: str = "row_hash") -> DataFrame:
        """Calculate hash of columns to detect changes"""
        # Coalesce nulls to empty string to ensure consistent hashing
        concat_expr = concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in cols])
        return df.withColumn(hash_col_name, md5(concat_expr))

    def apply_scd_type_2(self, source_df: DataFrame, table_name: str, keys: List[str], scd_cols: List[str]) -> DataFrame:
        """
        Apply SCD Type 2 logic to a dimension table.
        
        Args:
            source_df: New batch of data (snapshot)
            table_name: Target table name
            keys: Primary key columns (natural keys)
            scd_cols: Columns to monitor for changes
            
        Returns:
            DataFrame containing the new state of the dimension
        """
        target_path = Config.get_silver_path(table_name)
        logger.info(f"Applying SCD Type 2 for {table_name}")
        
        # Add hash to source
        source_hashed = self._calculate_hash(source_df, scd_cols, "new_hash")
        
        # Try to read existing target
        try:
            target_df = self.spark.read.parquet(target_path)
            logger.info(f"Loaded existing dimension from {target_path}")
        except Exception:
            logger.warning(f"Target {target_path} not found or empty. Initializing new dimension.")
            # If target doesn't exist, all source records are new
            final_df = source_hashed.withColumn("start_date", current_date()) \
                                    .withColumn("end_date", lit(None).cast("date")) \
                                    .withColumn("is_current", lit(True)) \
                                    .drop("new_hash")
            
            self._save_dimension(final_df, table_name, target_path)
            return final_df

        # --- SCD Logic (Full Snapshot Comparison) ---
        
        # 1. Separate Target into History (Closed) and Active
        target_closed = target_df.filter(col("is_current") == False)
        target_active = target_df.filter(col("is_current") == True)
        
        # 2. Compare Target Active vs Source
        # Calculate hashes for change detection
        target_active = self._calculate_hash(target_active, scd_cols, "old_hash")
        source_hashed = self._calculate_hash(source_df, scd_cols, "new_hash")
        
        # Join on Natural Keys
        cond = [target_active[k] == source_hashed[k] for k in keys]
        comparison = target_active.alias("t").join(source_hashed.alias("s"), cond, "full_outer")
        
        # 3. Categorize Records
        
        # A. UNCHANGED: Key Match AND Hash Match
        unchanged = comparison.filter(
            col("t.old_hash").isNotNull() & 
            col("s.new_hash").isNotNull() & 
            (col("t.old_hash") == col("s.new_hash"))
        ).select("t.*").drop("old_hash")
        
        # B. CHANGED (Old Version): Key Match AND Hash Diff -> Close it
        changed_old = comparison.filter(
            col("t.old_hash").isNotNull() & 
            col("s.new_hash").isNotNull() & 
            (col("t.old_hash") != col("s.new_hash"))
        ).select("t.*") \
         .withColumn("end_date", current_date()) \
         .withColumn("is_current", lit(False)) \
         .drop("old_hash")
         
        # C. CHANGED (New Version): Key Match AND Hash Diff -> Open new
        changed_new = comparison.filter(
            col("t.old_hash").isNotNull() & 
            col("s.new_hash").isNotNull() & 
            (col("t.old_hash") != col("s.new_hash"))
        ).select("s.*") \
         .withColumn("start_date", current_date()) \
         .withColumn("end_date", lit(None).cast("date")) \
         .withColumn("is_current", lit(True)) \
         .drop("new_hash")
         
        # D. NEW INSERT: Source Only
        new_inserts = comparison.filter(
            col("t.old_hash").isNull() & 
            col("s.new_hash").isNotNull()
        ).select("s.*") \
         .withColumn("start_date", current_date()) \
         .withColumn("end_date", lit(None).cast("date")) \
         .withColumn("is_current", lit(True)) \
         .drop("new_hash")
         
        # E. DELETED: Target Only (Active record missing in new snapshot) -> Close it
        deleted = comparison.filter(
            col("t.old_hash").isNotNull() & 
            col("s.new_hash").isNull()
        ).select("t.*") \
         .withColumn("end_date", current_date()) \
         .withColumn("is_current", lit(False)) \
         .drop("old_hash")

        # Combine All
        # Unions: target_closed + unchanged + changed_old + changed_new + new_inserts + deleted
        # Ensure schemas align
        
        final_df = target_closed.unionByName(unchanged, allowMissingColumns=True) \
                                .unionByName(changed_old, allowMissingColumns=True) \
                                .unionByName(changed_new, allowMissingColumns=True) \
                                .unionByName(new_inserts, allowMissingColumns=True) \
                                .unionByName(deleted, allowMissingColumns=True)
                                
        self._save_dimension(final_df, table_name, target_path)
        return final_df

    def _save_dimension(self, df: DataFrame, table_name: str, path: str):
        """Save optimized dimension table"""
        logger.info(f"Saving updated dimension {table_name} to {path}")
        df.write \
          .mode("overwrite") \
          .option("mergeSchema", "true") \
          .parquet(path)
