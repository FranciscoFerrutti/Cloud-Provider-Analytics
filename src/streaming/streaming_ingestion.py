"""
Structured Streaming ingestion for usage events (Speed Layer)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, to_date, year, month, dayofmonth,
    window, count as spark_count, sum as spark_sum, coalesce, lit, when, first
)
from pyspark.sql.streaming import StreamingQuery
import logging

from src.utils.config import Config
from src.utils.spark_utils import get_common_schemas
from src.utils.spark_utils import get_common_schemas
from src.quality.validators import DataQualityValidator
from src.serving.loader import CassandraLoader
from src.silver.transformations import SilverTransformations

logger = logging.getLogger(__name__)


class StreamingIngestion:
    """Handle streaming ingestion for usage events (Speed Layer)"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize streaming ingestion
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.validator = DataQualityValidator(Config.QUARANTINE_PATH)
        self.schemas = get_common_schemas()
        self.streaming_config = Config.STREAMING_CONFIG
        self.cassandra_loader = CassandraLoader(self.spark)
        self.silver_transformer = SilverTransformations(self.spark)
    
    def create_streaming_source(self, source_path: str) -> DataFrame:
        """
        Create streaming DataFrame from JSONL files
        
        Args:
            source_path: Path to streaming source directory
            
        Returns:
            Streaming DataFrame
        """
        logger.info(f"Creating streaming source from {source_path}")
        
        # Read streaming data with schema inference
        schema = self.schemas.get("usage_event")
        
        stream_df = self.spark.readStream \
            .schema(schema) \
            .option("maxFilesPerTrigger", self.streaming_config["max_files_per_trigger"]) \
            .option("latestFirst", "true") \
            .json(source_path)
        
        return stream_df
    
    def add_audit_fields(self, df: DataFrame) -> DataFrame:
        """
        Add audit fields to streaming data
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            DataFrame with audit fields
        """
        df = df.withColumn("ingest_ts", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        # Add partition columns
        df = df.withColumn("date", to_date(col("event_ts"))) \
               .withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date"))) \
               .withColumn("day", dayofmonth(col("date")))
        
        return df
    
    def deduplicate_streaming(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate streaming data by event_id within time windows
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        logger.info("Setting up deduplication for streaming")
        
        # Deduplicate using window and event_id
        window_spec = window(
            col("event_ts"),
            self.streaming_config["window_duration"],
            self.streaming_config["window_slide"]
        )
        
        # Group by window and event_id, take first occurrence
        deduplicated_df = df.withWatermark("event_ts", self.streaming_config["watermark_delay"]).groupBy(
            window_spec,
            col("event_id")
        ).agg(
            first("org_id").alias("org_id"),
            first("user_id").alias("user_id"),
            first("resource_id").alias("resource_id"),
            first("service").alias("service"),
            first("region").alias("region"),
            first("event_ts").alias("event_ts"),
            first("cost_usd_increment").alias("cost_usd_increment"),
            first("unit").alias("unit"),
            first("value").alias("value"),
            first("schema_version").alias("schema_version"),
            first("carbon_kg").alias("carbon_kg"),
            first("genai_tokens").alias("genai_tokens"),
            first("ingest_ts").alias("ingest_ts"),
            first("source_file").alias("source_file"),
            first("date").alias("date"),
            first("year").alias("year"),
            first("month").alias("month"),
            first("day").alias("day")
        )
        
        return deduplicated_df
    
    def validate_streaming(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validate streaming data and separate valid/invalid
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df) - Note: invalid_df needs to be handled separately
        """
        logger.info("Validating streaming data")
        
        # Create validation flags
        validation_df = df.withColumn(
            "is_valid",
            (col("event_id").isNotNull()) &
            (col("cost_usd_increment") >= -0.01) &
            ((col("value").isNull()) | (col("unit").isNotNull())) &
            (col("event_ts").isNotNull()) &
            (col("org_id").isNotNull())
        )
        
        # Separate valid and invalid
        valid_df = validation_df.filter(col("is_valid") == True).drop("is_valid")
        invalid_df = validation_df.filter(col("is_valid") == False).drop("is_valid")
        
        return valid_df, invalid_df
    
    def start_streaming_to_bronze(self, source_path: str = None, trigger: dict = None) -> list[StreamingQuery]:
        """
        Start streaming query to write to Bronze layer
        
        Args:
            source_path: Path to streaming source (defaults to config)
            trigger: Trigger configuration (defaults to processingTime from config)
            
        Returns:
            List of StreamingQuery instances (for invalid and valid streams)
        """
        if source_path is None:
            source_path = Config.LANDING_SOURCES["usage_events_stream"]
            
        if trigger is None:
            trigger = {"processingTime": self.streaming_config["trigger_interval"]}
        
        logger.info(f"Starting streaming ingestion from {source_path}")
        
        # Create streaming source
        stream_df = self.create_streaming_source(source_path)
        
        # Add audit fields
        stream_df = self.add_audit_fields(stream_df)
        
        # Validate and deduplicate
        valid_df, invalid_df = self.validate_streaming(stream_df)
        
        # Deduplicate valid records
        valid_df = self.deduplicate_streaming(valid_df)
        
        # Write valid data to Bronze
        bronze_path = Config.get_bronze_path("usage_events")
        checkpoint_path = f"{Config.STREAMING_CHECKPOINT}/bronze_usage_events"
        
        # Write invalid data to Quarantine
        quarantine_path = Config.get_quarantine_path("bronze", "usage_events")
        quarantine_checkpoint = f"{Config.STREAMING_CHECKPOINT}/quarantine_usage_events"
        
        logger.info(f"Starting quarantine stream to {quarantine_path}")
        
        # We need to write invalid_df to quarantine
        invalid_query = invalid_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", quarantine_path) \
            .option("checkpointLocation", quarantine_checkpoint) \
            .partitionBy("year", "month", "day") \
            .trigger(**trigger) \
            .start()
            
        logger.info(f"Quarantine streaming query started. Checkpoint: {quarantine_checkpoint}")
        
        query = valid_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", bronze_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("year", "month", "day") \
            .trigger(**trigger) \
            .start()
        
        logger.info(f"Streaming query started. Checkpoint: {checkpoint_path}")
        return [invalid_query, query]
    
    def start_streaming_silver(self) -> StreamingQuery:
        """
        Start streaming transformation from Bronze to Silver
        
        Returns:
            StreamingQuery instance
        """
        logger.info("Starting streaming Silver transformations")
        
        # Read from Bronze (streaming)
        bronze_path = Config.get_bronze_path("usage_events")
        
        # Create stream from Bronze files
        stream_df = self.spark.readStream \
            .schema(self.schemas.get("usage_event")) \
            .parquet(bronze_path)
            
        # Apply Silver Transformations (Reuse SilverTransformations logic where stream-safe)
        
        # 1. Normalizations
        df = self.silver_transformer.normalize_dates(stream_df)
        df = self.silver_transformer.normalize_regions(df)
        df = self.silver_transformer.normalize_services(df)
        
        # 2. Handle Nulls (Safe)
        df = self.silver_transformer.handle_nulls(df)
        
        # 3. Handle Outliers
        
        # 4. Enrichments (Safe - broadcast joins)
        df = self.silver_transformer.enrich_with_master_data(df)
        
        # Write to Silver Parquet (Stream)
        silver_path = Config.get_silver_path("usage_events_streaming")
        checkpoint_path = f"{Config.STREAMING_CHECKPOINT}/silver_usage_events_processing"
        
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", silver_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime="10 seconds") \
            .start()
            
        logger.info(f"Silver streaming query started. Checkpoint: {checkpoint_path}")
        return query

    def start_streaming_gold(self) -> StreamingQuery:
        """
        Start streaming aggregations from Silver to Gold and AstraDB
        
        Returns:
            StreamingQuery instance
        """
        logger.info("Starting streaming Gold aggregations")
        
        # Read from Silver streaming
        silver_path = Config.get_silver_path("usage_events_streaming")
        
        # Define minimal schema for aggregation to avoid UNABLE_TO_INFER_SCHEMA on empty dir
        # We only need columns used in aggregation
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
        
        silver_schema = StructType([
            StructField("event_ts", TimestampType(), True),
            StructField("org_id", StringType(), True),
            StructField("service", StringType(), True),
            StructField("region", StringType(), True),
            StructField("cost_usd_increment", DoubleType(), True),
            StructField("event_id", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("genai_tokens", LongType(), True),
            StructField("carbon_kg", DoubleType(), True)
        ])
        
        stream_df = self.spark.readStream \
            .schema(silver_schema) \
            .option("basePath", silver_path) \
            .parquet(silver_path)
        
        # Add watermark for late data handling
        stream_df = stream_df.withWatermark("event_ts", self.streaming_config["watermark_delay"])
        
        # Window aggregations
        window_spec = window(
            col("event_ts"),
            self.streaming_config["window_duration"],
            self.streaming_config["window_slide"]
        )
        
        # Aggregate by window, org, service
        aggregated_df = stream_df.groupBy(
            window_spec,
            col("org_id"),
            col("service"),
            col("region")
        ).agg(
            spark_sum("cost_usd_increment").alias("window_cost_usd"),
            spark_count("event_id").alias("window_requests"),
            spark_sum(
                when(col("unit") == "cpu_hours", col("value")).otherwise(lit(0))
            ).alias("window_cpu_hours"),
            spark_sum(
                when(col("unit") == "gb_hours", col("value")).otherwise(lit(0))
            ).alias("window_storage_gb_hours"),
            spark_sum(coalesce(col("genai_tokens"), lit(0))).alias("window_genai_tokens"),
            spark_sum(coalesce(col("carbon_kg"), lit(0))).alias("window_carbon_kg")
        )
        
        # Write to Gold streaming table AND AstraDB using foreachBatch
        gold_path = Config.get_gold_path("usage_events_streaming_agg")
        checkpoint_path = f"{Config.STREAMING_CHECKPOINT}/gold_usage_events_agg"
        
        def process_batch(batch_df: DataFrame, batch_id: int):
            # 1. Archive to Gold (Parquet)
            batch_df.write \
                .mode("append") \
                .parquet(gold_path)
        
        # Use update output mode for the query to get updates on windows
        query = aggregated_df.writeStream \
            .outputMode("update") \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=self.streaming_config["trigger_interval"]) \
            .start()
            
        logger.info(f"Gold streaming query started. Checkpoint: {checkpoint_path}")
        return query

