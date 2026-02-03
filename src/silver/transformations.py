"""
Silver layer transformations: normalization, enrichment, and feature engineering
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnull, coalesce, lit, sum as spark_sum, count, 
    avg, max as spark_max, min as spark_min, date_format, 
    to_date, year, month, dayofmonth, hour, upper, trim, regexp_replace,
    current_timestamp
)
from pyspark.sql.window import Window
import logging


from src.utils.config import Config
from src.quality.validators import DataQualityValidator
from src.silver.schema_manager import SchemaManager
from src.silver.scd_manager import SCDManager

logger = logging.getLogger(__name__)


class SilverTransformations:
    """Transformations for Silver layer"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Silver transformations
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.validator = DataQualityValidator(Config.QUARANTINE_PATH)
        self.schema_manager = SchemaManager(spark)
        self.scd_manager = SCDManager(spark)
    
    def normalize_dates(self, df: DataFrame, date_col: str = "event_ts") -> DataFrame:
        """
        Normalize date columns and extract date components
        
        Args:
            df: Input DataFrame
            date_col: Date column name
            
        Returns:
            DataFrame with normalized dates
        """
        logger.info(f"Normalizing dates from {date_col}")
        
        df = df.withColumn("date", to_date(col(date_col))) \
               .withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date"))) \
               .withColumn("day", dayofmonth(col("date"))) \
               .withColumn("hour", hour(col(date_col)))
        
        return df
    
    def normalize_regions(self, df: DataFrame, region_col: str = "region") -> DataFrame:
        """
        Normalize region values (uppercase, trim, standardize)
        
        Args:
            df: Input DataFrame
            region_col: Region column name
            
        Returns:
            DataFrame with normalized regions
        """
        logger.info(f"Normalizing regions")
        
        df = df.withColumn(
            region_col,
            upper(trim(regexp_replace(col(region_col), r"[^a-zA-Z0-9-]", "")))
        )
        
        return df
    
    def normalize_services(self, df: DataFrame, service_col: str = "service") -> DataFrame:
        """
        Normalize service names
        
        Args:
            df: Input DataFrame
            service_col: Service column name
            
        Returns:
            DataFrame with normalized services
        """
        logger.info(f"Normalizing services")
        
        df = df.withColumn(
            service_col,
            upper(trim(col(service_col)))
        )
        
        return df
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Handle null values with imputation strategies
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with handled nulls
        """
        logger.info("Handling null values")
        

        df = df.withColumn(
            "unit",
            when(col("unit").isNull() & col("service").isNotNull(), 
                 when(col("service").contains("COMPUTE"), "cpu_hours")
                 .when(col("service").contains("STORAGE"), "gb_hours")
                 .when(col("service").contains("NETWORK"), "gb_transferred")
                 .otherwise("requests"))
            .otherwise(col("unit"))
        )
        

        numeric_cols = ["cost_usd_increment", "value", "carbon_kg", "genai_tokens"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    coalesce(col(col_name), lit(0.0))
                )
        
        return df
    
    def handle_outliers(self, df: DataFrame, cost_col: str = "cost_usd_increment") -> DataFrame:
        """
        Handle outliers in cost data
        
        Args:
            df: Input DataFrame
            cost_col: Cost column name
            
        Returns:
            DataFrame with outlier flags
        """
        logger.info("Handling outliers")
        

        cost_stats = df.select(
            spark_max(col(cost_col)).alias("max_cost"),
            spark_min(col(cost_col)).alias("min_cost")
        ).collect()[0]
        

        df = df.withColumn(
            "is_outlier",
            (col(cost_col) < 0) | (col(cost_col) > cost_stats["max_cost"] * 0.1)
        )
        

        df = df.withColumn(
            cost_col,
            when(col("is_outlier"), 
                 when(col(cost_col) < 0, lit(0.0))
                 .otherwise(cost_stats["max_cost"] * 0.1))
            .otherwise(col(cost_col))
        )
        
        return df
    
    def enrich_with_master_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich usage events with master data (orgs, users, resources)
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching with master data")
        

        orgs_df = self.spark.read.parquet(Config.get_bronze_path("customers_orgs"))
        users_df = self.spark.read.parquet(Config.get_bronze_path("users"))
        resources_df = self.spark.read.parquet(Config.get_bronze_path("resources"))
        

        df = df.join(
            orgs_df.select("org_id", "org_name", "industry", "tier"),
            on="org_id",
            how="left"
        )
        

        df = df.join(
            users_df.select("user_id", "email", "role").alias("user_info"),
            on="user_id",
            how="left"
        )
        

        df = df.join(
            resources_df.select("resource_id", "resource_type").alias("resource_info"),
            on="resource_id",
            how="left"
        )
        
        return df
    
    def calculate_daily_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate daily aggregated metrics
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with daily metrics
        """
        logger.info("Calculating daily metrics")
        

        daily_metrics = df.groupBy(
            "date", "year", "month", "day",
            "org_id", "service", "region"
        ).agg(
            spark_sum("cost_usd_increment").alias("daily_cost_usd"),
            count("event_id").alias("requests"),
            spark_sum(
                when(col("unit") == "cpu_hours", col("value"))
                .otherwise(lit(0))
            ).alias("cpu_hours"),
            spark_sum(
                when(col("unit") == "gb_hours", col("value"))
                .otherwise(lit(0))
            ).alias("storage_gb_hours"),
            spark_sum(coalesce(col("genai_tokens"), lit(0))).alias("genai_tokens"),
            spark_sum(coalesce(col("carbon_kg"), lit(0))).alias("carbon_kg")
        )
        
        return daily_metrics
    
    def transform_usage_events(self, df: DataFrame) -> DataFrame:
        """
        Complete transformation pipeline for usage events
        
        Args:
            df: Input DataFrame from Bronze
            
        Returns:
            Transformed DataFrame for Silver
        """
        logger.info("Starting Silver transformation pipeline")
        

        df = self.apply_schema_evolution(df, "usage_events")


        df = self.normalize_dates(df)
        df = self.normalize_regions(df)
        df = self.normalize_services(df)
        

        df = self.handle_nulls(df)
        df = self.handle_outliers(df)
        

        df = self.enrich_with_master_data(df)


        df = self.enrich_with_exchange_rates(df)
        

        df = self.calculate_daily_metrics(df)
        
        logger.info("Silver transformation pipeline completed")
        return df

    def transform_billing_data(self) -> DataFrame:
        """
        Transform and save billing data to Silver
        """
        logger.info("Starting billing_monthly Silver transformation")
        

        df = self.spark.read.parquet(Config.get_bronze_path("billing_monthly"))
        
        logger.info(f"Initial billing count: {df.count()}")
        

        df = df.withColumn("credits", coalesce(col("credits"), lit(0.0))) \
               .withColumn("taxes", coalesce(col("taxes"), lit(0.0)))


        df = df.withColumn("billing_date", to_date(col("billing_period"))) \
               .withColumn("year", year(col("billing_date"))) \
               .withColumn("month", month(col("billing_date")))


        df = df.withColumn("exchange_rate_to_usd", col("exchange_rate_to_usd").cast("double"))
        ex_rate = coalesce(col("exchange_rate_to_usd"), lit(1.0))
        
        df = df.withColumn("amount_usd", col("subtotal") * ex_rate) \
               .withColumn("credits_usd", col("credits") * ex_rate) \
               .withColumn("taxes_usd", col("taxes") * ex_rate) \
               .withColumn("net_amount", col("amount_usd") - col("credits_usd") + col("taxes_usd")) \
               .withColumn("currency", lit("USD"))
               
        df = df.withColumn("is_valid", 
                           col("invoice_id").isNotNull() & 
                           col("org_id").isNotNull() & 
                           (col("amount_usd") >= -1000000))
        
        df = df.withColumn("silver_processing_timestamp", current_timestamp())
        
        valid_df = df.filter(col("is_valid") == True).drop("is_valid")
        invalid_df = df.filter(col("is_valid") == False).drop("is_valid")
        
        self.validator.save_quarantine(invalid_df, "silver", "billing_monthly")
        

        silver_path = Config.get_silver_path("billing_processed")
        valid_df.write.mode("overwrite").parquet(silver_path)
        logger.info(f"billing_monthly Silver transformation completed. Saved to {silver_path}")
        
        return valid_df

    def enrich_with_exchange_rates(self, df: DataFrame) -> DataFrame:
        """
        Enrich usage events with exchange rates from billing_processed
        """
        logger.info("Enriching usage events with exchange rates")
        

        try:
            billing_df = self.spark.read.parquet(Config.get_silver_path("billing_processed"))
        except Exception as e:
            logger.warning(f"Could not read billing_processed: {e}. Using default exchange rate 1.0")
            return df.withColumn("exchange_rate_to_usd", lit(1.0)) \
                     .withColumn("cost_usd_real", col("cost_usd_increment"))


        rates = billing_df.select("org_id", "year", "month", "exchange_rate_to_usd")
        

        joined_df = df.join(rates, on=["org_id", "year", "month"], how="left")
        

        joined_df = joined_df.withColumn("exchange_rate_to_usd", coalesce(col("exchange_rate_to_usd"), lit(1.0)))
        

        
        # Double conversion removed: cost_usd_increment is already in USD.
                             
        return joined_df

    def apply_schema_evolution(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply schema standardization
        """
        return self.schema_manager.standardize_schema(df, table_name)
    
    def process_dimension_scd(self, df: DataFrame, table_name: str, keys: list) -> DataFrame:
        """
        Process dimension table with SCD Type 2
        """

        # Exclude metadata columns from SCD change detection and drop them to avoid VoidType errors
        metadata_cols = ["ingest_ts", "source_file", "date", "year", "month", "day"]
        scd_cols = [c for c in df.columns if c not in keys and c not in metadata_cols]
        
        # Drop metadata columns as they shouldn't be part of the dimension table
        for col_name in metadata_cols:
            if col_name in df.columns:
                df = df.drop(col_name)
        
        return self.scd_manager.apply_scd_type_2(df, table_name, keys, scd_cols)
    
    def save_to_silver(self, df: DataFrame, table_name: str):
        """
        Save transformed data to Silver layer
        
        Args:
            df: Transformed DataFrame
            table_name: Target table name
        """
        silver_path = Config.get_silver_path(table_name)
        logger.info(f"Saving to Silver: {silver_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day", "service") \
            .option("mergeSchema", "true") \
            .parquet(silver_path)
        
        logger.info(f"Successfully saved {df.count()} records to Silver/{table_name}")

