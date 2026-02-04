"""
Main pipeline orchestration
"""

from pyspark.sql import SparkSession
import logging

from src.utils.spark_utils import create_spark_session, get_common_schemas
from src.utils.logger import setup_logging
from src.utils.config import Config
from src.ingestion.batch_ingestion import BatchIngestion
from src.streaming.streaming_ingestion import StreamingIngestion
from src.silver.transformations import SilverTransformations
from src.silver.anomaly_detection import AnomalyDetector
from src.gold.marts import GoldMarts
from src.serving.loader import CassandraLoader

logger = logging.getLogger(__name__)


class Pipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, spark: SparkSession = None):
        """
        Initialize pipeline
        
        Args:
            spark: Optional SparkSession (creates new if not provided)
        """
        if spark is None:
            self.spark = create_spark_session()
        else:
            self.spark = spark
        
        self.batch_ingestion = BatchIngestion(self.spark)
        self.streaming_ingestion = StreamingIngestion(self.spark)
        self.silver_transforms = SilverTransformations(self.spark)
        self.anomaly_detector = AnomalyDetector(self.spark)
        self.gold_marts = GoldMarts(self.spark)
        self.cassandra_loader = CassandraLoader(self.spark)
    
    def run_batch_layer(self):
        """Run batch layer processing"""
        logger.info("=" * 50)
        logger.info("Starting Batch Layer Processing")
        logger.info("=" * 50)
        
        # Ingest all data from Landing (Master Data + Usage Events)
        self.batch_ingestion.ingest_landing_to_bronze()
        
        logger.info("Batch Layer Processing Completed")
    
    def run_speed_layer(self):
        """Run speed layer (streaming) processing"""
        logger.info("=" * 50)
        logger.info("Starting Speed Layer Processing")
        logger.info("=" * 50)
        
        # Start streaming ingestion
        ingestion_queries = self.streaming_ingestion.start_streaming_to_bronze()
        
        # Start streaming transformations (Bronze -> Silver)
        silver_query = self.streaming_ingestion.start_streaming_silver()
        
        # Start streaming aggregations (Silver -> Gold + AstraDB)
        gold_query = self.streaming_ingestion.start_streaming_gold()
        
        logger.info("Speed Layer Processing Started")
        logger.info("Streaming queries (Ingestion, Silver, Gold) are running. Use spark.streams.awaitAnyTermination() to wait.")
        
        # Return flattened list of queries
        return ingestion_queries + [silver_query, gold_query]
    
    def run_silver_layer(self):
        """Run Silver layer transformations"""
        logger.info("=" * 50)
        logger.info("Starting Silver Layer Processing")
        logger.info("=" * 50)
        
        # --- Process Billing Data (Required for Usage Events USD Conversion) ---
        self.silver_transforms.transform_billing_data()

        # Read from Bronze
        # Use schema to avoid AnalysisException if directory is empty (e.g. initial run)
        usage_schema = get_common_schemas().get("usage_event")
        bronze_df = self.spark.read.schema(usage_schema).parquet(Config.get_bronze_path("usage_events"))
        
        # Transform
        silver_df = self.silver_transforms.transform_usage_events(bronze_df)
        
        # Detect anomalies
        silver_df = self.anomaly_detector.detect_all_anomalies(silver_df, "daily_cost_usd")
        
        # Save to Silver
        self.silver_transforms.save_to_silver(silver_df, "usage_events")
        
        # --- Process Support Tickets ---
        logger.info("Processing Support Tickets (Silver)")
        bronze_tickets = self.spark.read.parquet(Config.get_bronze_path("support_tickets"))
        silver_tickets = self.silver_transforms.transform_support_tickets(bronze_tickets)
        self.silver_transforms.save_to_silver(silver_tickets, "support_tickets", ["year", "month", "day"])
        
        logger.info("Silver Layer Processing Completed")
        
        # --- Process Dimensions with SCD Type 2 ---
        logger.info("Processing Silver Dimensions with SCD Type 2")
        
        # 1. Organizations
        bronze_orgs = self.spark.read.parquet(Config.get_bronze_path("customers_orgs"))
        silver_orgs = self.silver_transforms.process_dimension_scd(
            bronze_orgs, "customers_orgs", keys=["org_id"]
        )
        
        # 2. Users
        bronze_users = self.spark.read.parquet(Config.get_bronze_path("users"))
        silver_users = self.silver_transforms.process_dimension_scd(
            bronze_users, "users", keys=["user_id"]
        )
        
        # 3. Resources
        bronze_resources = self.spark.read.parquet(Config.get_bronze_path("resources"))
        silver_resources = self.silver_transforms.process_dimension_scd(
            bronze_resources, "resources", keys=["resource_id"]
        )
        
        logger.info("Silver Dimensions Processing Completed")
    
    def run_gold_layer(self):
        """Run Gold layer mart creation"""
        logger.info("=" * 50)
        logger.info("Starting Gold Layer Processing")
        logger.info("=" * 50)
        
        # Create and save all marts
        self.gold_marts.save_all_marts()
        
        logger.info("Gold Layer Processing Completed")
    
    def run_serving_layer(self):
        """Run serving layer (Cassandra load)"""
        logger.info("=" * 50)
        logger.info("Starting Serving Layer Processing")
        logger.info("=" * 50)
        
        # Create schema
        self.cassandra_loader.create_keyspace_and_tables()
        
        # Load all marts
        self.cassandra_loader.load_all_marts()
        
        logger.info("Serving Layer Processing Completed")
    
    def run_full_pipeline(self):
        """Run complete Lambda Architecture pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Full Lambda Architecture Pipeline")
        logger.info("=" * 60)
        
        # Batch Layer
        self.run_batch_layer()
        
        # Silver Layer (from batch)
        self.run_silver_layer()
        
        # Gold Layer
        self.run_gold_layer()
        
        # Serving Layer
        self.run_serving_layer()
        
        logger.info("=" * 60)
        logger.info("Full Pipeline Completed Successfully")
        logger.info("=" * 60)
        
        # Note: Speed Layer (streaming) should be started separately
        # as it runs continuously
        logger.info("Note: Start Speed Layer separately using run_speed_layer()")

