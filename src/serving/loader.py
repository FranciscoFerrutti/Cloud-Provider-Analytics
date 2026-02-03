"""
Load data from Gold layer to AstraDB Collections
Decoupled implementation using astrapy DataAPIClient.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
import logging
import os
from typing import Iterator

from src.utils.config import Config
from src.serving.astradb_setup import AstraDBSetup

logger = logging.getLogger(__name__)


def write_partition_to_astra(partition: Iterator, collection_name: str, config: dict):
    """
    Write a partition of rows to AstraDB Collection using DataAPIClient.
    Designed to run on executors.
    
    Args:
        partition: Iterator of Row objects
        collection_name: Target collection name
        config: AstraDB connection config
    """
    try:
        from astrapy.client import DataAPIClient
    except ImportError:
        # Should be installed on workers
        return

    db_id = config.get("db_id")
    region = config.get("region")
    token = config.get("token")
    keyspace = config.get("keyspace")
    
    if not db_id or not region or not token:
        # Log failure (print to executor stderr)
        print("Missing AstraDB credentials on executor.")
        return

    # Construct Endpoint
    api_endpoint = f"https://{db_id}-{region}.apps.astra.datastax.com"

    # Connect to AstraDB
    client = DataAPIClient(token=token)
    db = client.get_database(api_endpoint, keyspace=keyspace)
    
    try:
        collection = db.get_collection(collection_name)
    except Exception:
        try:
             collection = db.create_collection(collection_name)
        except Exception:
             print(f"Could not get collection {collection_name}")
             return

    # Insert loop
    for row in partition:
        doc = row.asDict()
        
        # Serialize dates/types
        for k, v in doc.items():
            if hasattr(v, 'isoformat'):
                doc[k] = v.isoformat()
        
        try:
            # Using insert_one as per DataAPIClient docstring example
            collection.insert_one(doc)
        except Exception as e:
            print(f"Error inserting document into {collection_name}: {e}")
            # Optional: Retry logic


class CassandraLoader:
    """Load Gold marts to AstraDB Collections"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Loader
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.cassandra_config = Config.CASSANDRA_CONFIG
    
    def create_keyspace_and_tables(self):
        """Initialize Collections"""
        logger.info("Initializing AstraDB Collections")
        AstraDBSetup.setup_infrastructure()
    
    def load_mart_to_cassandra(self, mart_name: str, table_name: str = None):
        """
        Load a Gold mart to AstraDB Collection
        
        Args:
            mart_name: Name of the mart in Gold layer
            table_name: Target collection name (defaults to mart_name)
        """
        if table_name is None:
            table_name = mart_name
        
        logger.info(f"Loading {mart_name} to AstraDB Collection {table_name}")
        
        # Read from Gold
        gold_path = Config.get_gold_path(mart_name)
        df = self.spark.read.parquet(gold_path)
        
        # Capture config
        config_broadcast = self.cassandra_config
        
        # Execute foreachPartition
        try:
            df.foreachPartition(
                lambda partition: write_partition_to_astra(partition, table_name, config_broadcast)
            )
            logger.info(f"Successfully loaded {mart_name} to Astra/{table_name}")
        except Exception as e:
            logger.error(f"Error loading {mart_name} to Astra: {e}")
            raise

    def load_all_marts(self):
        """Load all Gold marts to AstraDB"""
        logger.info("Loading all Gold marts to AstraDB")
        
        marts = [
            "org_daily_usage_by_service",
            "revenue_by_org_month",
            "cost_anomaly_mart",
            "tickets_by_org_date",
            "genai_tokens_by_org_date"
        ]
        
        for mart_name in marts:
            try:
                self.load_mart_to_cassandra(mart_name)
            except Exception as e:
                logger.error(f"Failed to load {mart_name}: {e}")
                continue
    
    def write_batch_to_astra(self, df: DataFrame, collection_name: str):
        """
        Write a batch DataFrame to AstraDB (for Streaming)
        
        Args:
            df: Batch DataFrame
            collection_name: Target collection name
        """
        config_broadcast = self.cassandra_config
        
        try:
            df.foreachPartition(
                lambda partition: write_partition_to_astra(partition, collection_name, config_broadcast)
            )
            # logger.info(f"Written batch to Astra/{collection_name}") # Too noisy for streaming
        except Exception as e:
            logger.error(f"Error writing batch to Astra/{collection_name}: {e}")
            # Don't raise, let stream continue? Or raise to fail query?
            # Better to log and continue or depend on checkpoint to retry
            raise
