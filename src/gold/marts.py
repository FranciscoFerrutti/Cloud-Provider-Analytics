"""
Gold layer business marts for analytics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    date_format, to_date, year, month, dayofmonth, when, coalesce, lit, concat
)
import logging

from src.utils.config import Config

logger = logging.getLogger(__name__)


class GoldMarts:
    """Create business marts for analytics"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Gold marts
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def create_org_daily_usage_by_service(self) -> DataFrame:
        """
        Create FinOps mart: Daily usage by organization and service
        
        Returns:
            DataFrame with daily usage metrics
        """
        logger.info("Creating org_daily_usage_by_service mart")
        

        
        silver_df = self.spark.read.parquet(Config.get_silver_path("usage_events"))
        
        mart_df = silver_df.groupBy(
            "org_id",
            "date",
            "year",
            "month",
            "day",
            "service",
            "region"
        ).agg(
            spark_sum("daily_cost_usd").alias("total_cost_usd"),
            spark_sum("requests").alias("total_requests"),
            spark_sum("cpu_hours").alias("total_cpu_hours"),
            spark_sum("storage_gb_hours").alias("total_storage_gb_hours"),
            spark_sum("genai_tokens").alias("total_genai_tokens"),
            spark_sum("carbon_kg").alias("total_carbon_kg")
        )
        
        return mart_df
    
    def create_revenue_by_org_month(self) -> DataFrame:
        """
        Create FinOps mart: Monthly revenue by organization (sourced from Billing)
        """
        logger.info("Creating revenue_by_org_month mart")
        
        billing_df = self.spark.read.parquet(Config.get_silver_path("billing_processed"))
        
        usage_df = self.spark.read.parquet(Config.get_silver_path("usage_events"))
        
        usage_stats = usage_df.groupBy("org_id", "year", "month").agg(
            spark_sum("requests").alias("monthly_requests"),
            spark_sum("cpu_hours").alias("monthly_cpu_hours"),
            spark_sum("storage_gb_hours").alias("monthly_storage_gb_hours"),
            spark_sum("genai_tokens").alias("monthly_genai_tokens"),
            spark_sum("carbon_kg").alias("monthly_carbon_kg"),
            count("date").alias("active_days")
        )
        
        billing_mart = billing_df.select(
            col("org_id"),
            col("year"),
            col("month"),
            lit(1).alias("day"),
            col("amount_usd").alias("subtotal_usd"),
            col("credits_usd"),
            col("taxes_usd"),
            col("net_amount").alias("total_revenue_usd"),
            col("exchange_rate_to_usd").alias("exchange_rate_used")
        )
        
        mart_df = billing_mart.join(usage_stats, on=["org_id", "year", "month"], how="left")
        
        return mart_df
    
    def create_cost_anomaly_mart(self) -> DataFrame:
        """
        Create FinOps mart: Cost anomalies detected
        
        Returns:
            DataFrame with anomaly records
        """
        logger.info("Creating cost_anomaly_mart")
        
        silver_df = self.spark.read.parquet(Config.get_silver_path("usage_events"))
        
        anomaly_df = silver_df.filter(
            col("is_anomaly") == True
        ).select(
            "org_id",
            "date",
            "year",
            "month",
            "day",
            "service",
            "region",
            "daily_cost_usd",
            "is_anomaly_zscore",
            "is_anomaly_mad",
            "is_anomaly_percentile",
            "z_score",
            "mad_score",
            "percentile_threshold"
        )
        
        return anomaly_df
    
    def create_tickets_by_org_date(self) -> DataFrame:
        """
        Create Support mart: Tickets by organization and date
        
        Returns:
            DataFrame with ticket metrics
        """
        logger.info("Creating tickets_by_org_date mart")
        
        tickets_df = self.spark.read.parquet(Config.get_bronze_path("support_tickets"))
        
        tickets_df = tickets_df.withColumn(
            "date",
            to_date(col("created_at"))
        ).withColumn(
            "year",
            year(col("date"))
        ).withColumn(
            "month",
            month(col("date"))
        ).withColumn(
            "day",
            dayofmonth(col("date"))
        )
        
        tickets_df = tickets_df.withColumn(
            "resolution_hours",
            when(
                col("resolved_at").isNotNull(),
                (col("resolved_at").cast("long") - col("created_at").cast("long")) / 3600.0
            ).otherwise(None)
        )
        
        mart_df = tickets_df.groupBy(
            "org_id",
            "date",
            "year",
            "month",
            "day"
        ).agg(
            count("ticket_id").alias("ticket_count"),
            avg("csat_score").alias("avg_csat_score"),
            spark_sum(when(col("sla_breach") == True, 1).otherwise(0)).alias("sla_breach_count"),
            (spark_sum(when(col("sla_breach") == True, 1).otherwise(0)) / count("ticket_id") * 100).alias("sla_breach_rate"),
            avg("resolution_hours").alias("avg_resolution_hours"),
            spark_sum(when(col("priority") == "critical", 1).otherwise(0)).alias("critical_tickets_count")
        )
        
        return mart_df
    
    def create_genai_tokens_by_org_date(self) -> DataFrame:
        """
        Create Product/GenAI mart: GenAI tokens by organization and date (aggregated)
        """
        logger.info("Creating genai_tokens_by_org_date mart")
        
        silver_df = self.spark.read.parquet(Config.get_silver_path("usage_events"))
        
        genai_df = silver_df.filter(
            (col("service").contains("GENAI")) |
            (col("genai_tokens") > 0)
        )
        
        mart_df = genai_df.groupBy(
            "org_id",
            "date",
            "year",
            "month",
            "day"
        ).agg(
            spark_sum("genai_tokens").alias("total_tokens"),
            spark_sum("daily_cost_usd").alias("genai_cost_usd"),
            spark_sum("requests").alias("genai_requests")
        )
        
        return mart_df
    
    def save_all_marts(self):
        """Save all Gold marts to Parquet"""
        logger.info("Saving all Gold marts")
        
        marts = {
            "org_daily_usage_by_service": self.create_org_daily_usage_by_service(),
            "revenue_by_org_month": self.create_revenue_by_org_month(),
            "cost_anomaly_mart": self.create_cost_anomaly_mart(),
            "tickets_by_org_date": self.create_tickets_by_org_date(),
            "genai_tokens_by_org_date": self.create_genai_tokens_by_org_date()
        }
        
        for mart_name, mart_df in marts.items():
            gold_path = Config.get_gold_path(mart_name)
            logger.info(f"Saving {mart_name} to {gold_path}")
            
            mart_df.write \
                .mode("overwrite") \
                .partitionBy("year", "month", "day") \
                .option("mergeSchema", "true") \
                .parquet(gold_path)
            
            logger.info(f"Successfully saved {mart_df.count()} records to Gold/{mart_name}")

