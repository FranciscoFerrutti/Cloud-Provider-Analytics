"""
Batch ingestion from Landing to Bronze layer
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_date, year, month, dayofmonth, lit
import logging

from src.utils.config import Config
from src.utils.spark_utils import get_common_schemas
from src.quality.validators import DataQualityValidator
from src.streaming.streaming_ingestion import StreamingIngestion
import time

logger = logging.getLogger(__name__)


class BatchIngestion:
    """Handle batch ingestion from CSV files to Bronze layer"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize batch ingestion
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.validator = DataQualityValidator(Config.QUARANTINE_PATH)
        self.schemas = get_common_schemas()
    
    def ingest_csv_to_bronze(self, source_name: str, source_path: str, 
                            table_name: str, schema: dict = None, read_options: dict = None) -> DataFrame:
        """
        Ingest CSV file to Bronze layer with type standardization and audit fields
        
        Args:
            source_name: Name of the source (for logging)
            source_path: Path to CSV file
            table_name: Target table name in Bronze
            schema: Optional schema definition
            
        Returns:
            DataFrame with ingested data
        """
        logger.info(f"Ingesting {source_name} from {source_path} to Bronze")
        
        # Read CSV with schema if provided
        if schema:
            df = self.spark.read \
                .schema(schema) \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .options(**(read_options or {})) \
                .csv(source_path)
        else:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .options(**(read_options or {})) \
                .csv(source_path)
        
        # Add audit fields
        df = df.withColumn("ingest_ts", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        # Handle billing_monthly column collision
        if table_name == "billing_monthly" and "month" in df.columns:
            df = df.withColumnRenamed("month", "billing_period")
        
        # Add partition columns for date-based partitioning
        if "created_at" in df.columns:
            df = df.withColumn("date", to_date(col("created_at"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "event_ts" in df.columns:
            df = df.withColumn("date", to_date(col("event_ts"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "billing_period" in df.columns:
            df = df.withColumn("date", to_date(col("billing_period"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "signup_date" in df.columns:
            df = df.withColumn("date", to_date(col("signup_date"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "survey_date" in df.columns:
            df = df.withColumn("date", to_date(col("survey_date"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "timestamp" in df.columns:
            df = df.withColumn("date", to_date(col("timestamp"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        else:
            # Default to current date if no timestamp column
            df = df.withColumn("date", to_date(current_timestamp())) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        
        # Validate data quality
        if table_name == "usage_events":
            valid_df, invalid_df = self.validator.validate_usage_events(df)
            self.validator.save_quarantine(invalid_df, "bronze", table_name)
            df = valid_df
        
        # Deduplicate if event_id exists
        if "event_id" in df.columns:
            df = self.validator.deduplicate_by_event_id(df)
        
        # Save to Bronze
        bronze_path = Config.get_bronze_path(table_name)
        logger.info(f"Saving to Bronze: {bronze_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .option("mergeSchema", "true") \
            .parquet(bronze_path)
        
        logger.info(f"Successfully ingested {df.count()} records to Bronze/{table_name}")
        return df
    
    def ingest_json_to_bronze(self, source_name: str, source_path: str, 
                            table_name: str, schema: dict = None) -> DataFrame:
        """
        Ingest JSON/JSONL file(s) to Bronze layer with type standardization and audit fields
        
        Args:
            source_name: Name of the source (for logging)
            source_path: Path to JSON/JSONL file(s)
            table_name: Target table name in Bronze
            schema: Optional schema definition
            
        Returns:
            DataFrame with ingested data
        """
        logger.info(f"Ingesting {source_name} from {source_path} to Bronze")
        
        # Read JSON (infer schema first to allow column manipulation)
        df = self.spark.read.json(source_path)
        
        if "timestamp" in df.columns and "event_ts" not in df.columns:
             df = df.withColumnRenamed("timestamp", "event_ts")
             logger.info("Renamed 'timestamp' column to 'event_ts'")
        
        if schema:
            schema_cols = []
            for field in schema.fields:
                if field.name in df.columns:
                    schema_cols.append(col(field.name).cast(field.dataType))
                else:
                    logger.warning(f"Column {field.name} missing from source, filling with null")
                    schema_cols.append(lit(None).cast(field.dataType).alias(field.name))
            
            df = df.select(schema_cols)
        
        # Add audit fields
        df = df.withColumn("ingest_ts", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        # Add partition columns for date-based partitioning
        if "created_at" in df.columns:
            df = df.withColumn("date", to_date(col("created_at"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        elif "event_ts" in df.columns:
            df = df.withColumn("date", to_date(col("event_ts"))) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        else:
            # Default to current date if no timestamp column
            df = df.withColumn("date", to_date(current_timestamp())) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date")))
        
        # Validate data quality
        if table_name == "usage_events":
            valid_df, invalid_df = self.validator.validate_usage_events(df)
            self.validator.save_quarantine(invalid_df, "bronze", table_name)
            df = valid_df
        
        # Deduplicate if event_id exists
        if "event_id" in df.columns:
            df = self.validator.deduplicate_by_event_id(df)
        
        # Save to Bronze
        bronze_path = Config.get_bronze_path(table_name)
        logger.info(f"Saving to Bronze: {bronze_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .option("mergeSchema", "true") \
            .parquet(bronze_path)
        
        logger.info(f"Successfully ingested {df.count()} records to Bronze/{table_name}")
        return df

    def ingest_landing_to_bronze(self):
        """
        Ingest all data from Landing to Bronze (Batch Layer)
        Includes master data (CSV) and historical usage events (JSONL)
        """
        logger.info("Starting batch ingestion from Landing to Bronze")
        
        schemas = self.schemas
        
        # 1. Ingest Master Data (CSV)
        self.ingest_csv_to_bronze(
            "customers_orgs",
            Config.LANDING_SOURCES["customers_orgs"],
            "customers_orgs",
            None
        )
        
        self.ingest_csv_to_bronze(
            "users",
            Config.LANDING_SOURCES["users"],
            "users",
            None
        )
        
        self.ingest_csv_to_bronze(
            "resources",
            Config.LANDING_SOURCES["resources"],
            "resources",
            None
        )
        
        self.ingest_csv_to_bronze(
            "support_tickets",
            Config.LANDING_SOURCES["support_tickets"],
            "support_tickets",
            schemas.get("support_ticket"),
            read_options={"dateFormat": "yyyy-MM-dd"}
        )
        
        self.ingest_csv_to_bronze(
            "marketing_touches",
            Config.LANDING_SOURCES["marketing_touches"],
            "marketing_touches",
            None
        )
        
        self.ingest_csv_to_bronze(
            "nps_surveys",
            Config.LANDING_SOURCES["nps_surveys"],
            "nps_surveys",
            None
        )
        
        self.ingest_csv_to_bronze(
            "billing_monthly",
            Config.LANDING_SOURCES["billing_monthly"],
            "billing_monthly",
            schemas.get("billing_monthly")
        )
        
        # 2. Ingest Historical Usage Events (JSONL) using Unified Streaming Logic
        # Ingest Historical Usage Events (JSONL) using Static Batch Read (Optimization for Colab)
        logger.info("Ingesting usage events using Static Batch Read (No Streaming)")
        streaming_ingestion = StreamingIngestion(self.spark)
        
        # Read JSONL directly using the landing schema (EAV)
        landing_path = Config.LANDING_SOURCES["usage_events"]
        landing_schema = get_common_schemas()["usage_event_landing"]
        
        logger.info(f"Reading JSONL from {landing_path}")
        raw_df = self.spark.read.schema(landing_schema).json(landing_path)
        
        if not raw_df.isEmpty():
            # Apply transformation (EAV pivot etc) reuse existing logic
            standard_df = streaming_ingestion.transform_landing_to_standard(raw_df)
            
            # Deduplicate (Batch style - simple dropDuplicates)
            # We don't use streaming deduplication (watermarks) here, just unique event_id
            dedup_df = standard_df.dropDuplicates(["event_id"])
            
            # Write to Bronze (Partitioned)
            bronze_output = Config.get_bronze_path("usage_events")
            logger.info(f"Writing records to Bronze: {bronze_output}")
            
            dedup_df.write \
                .mode("overwrite") \
                .partitionBy("year", "month", "day") \
                .parquet(bronze_output)
            
            logger.info("Static batch ingestion completed successfully")
        else:
             logger.warning("No data found in usage_events landing path")
        
        logger.info("Batch ingestion from Landing to Bronze completed")

