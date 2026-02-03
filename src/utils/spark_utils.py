"""
Spark session management and utilities
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType, DateType
import logging
import os

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "CloudProviderAnalytics", 
                         master: str = "local[*]",
                         config: dict = None) -> SparkSession:
    """
    Create and configure Spark session for Google Colab environment
    
    Args:
        app_name: Application name
        master: Spark master URL
        config: Additional Spark configuration dictionary
    
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Default configurations (works in Docker/Colab/Linux)
    import tempfile
    temp_dir = tempfile.gettempdir()
    
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.warehouse.dir": os.path.join(temp_dir, "spark-warehouse"),
        "spark.sql.streaming.checkpointLocation": os.path.join(temp_dir, "checkpoints"),
        "spark.sql.streaming.schemaInference": "true",
        "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
        "spark.sql.shuffle.partitions": "200",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    if config:
        default_config.update(config)
    
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    logger.info(f"Spark session created: {app_name}")
    return spark


def get_common_schemas():
    """
    Define common schemas for data sources
    
    Returns:
        Dictionary of schema definitions
    """
    schemas = {
        "usage_event": StructType([
            StructField("event_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("resource_id", StringType(), True),
            StructField("service", StringType(), True),
            StructField("region", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("cost_usd_increment", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("schema_version", IntegerType(), True),
            StructField("carbon_kg", DoubleType(), True),  # v2
            StructField("genai_tokens", LongType(), True)  # GenAI
        ]),
        
        "customer_org": StructType([
            StructField("org_id", StringType(), False),
            StructField("org_name", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]),
        
        "user": StructType([
            StructField("user_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("role", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]),
        
        "resource": StructType([
            StructField("resource_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("resource_type", StringType(), True),
            StructField("service", StringType(), True),
            StructField("region", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]),
        
        "support_ticket": StructType([
            StructField("ticket_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("created_at", DateType(), True),
            StructField("resolved_at", DateType(), True),
            StructField("priority", StringType(), True),
            StructField("status", StringType(), True),
            StructField("csat_score", IntegerType(), True),
            StructField("sla_breach", BooleanType(), True)
        ]),
        
        "billing_monthly": StructType([
            StructField("invoice_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("month", StringType(), True),
            StructField("subtotal", DoubleType(), True),
            StructField("credits", DoubleType(), True),
            StructField("taxes", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("exchange_rate_to_usd", DoubleType(), True)
        ]),
        
        "nps_survey": StructType([
            StructField("survey_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("feedback", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]),
        
        "marketing_touch": StructType([
            StructField("touch_id", StringType(), False),
            StructField("org_id", StringType(), True),
            StructField("touch_type", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
    }
    
    return schemas

