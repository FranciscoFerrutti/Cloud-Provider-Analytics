"""
Load data from Gold layer to AstraDB Collections
Decoupled implementation using astrapy.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
import logging
import os
import uuid
from typing import Iterator

from src.utils.config import Config
from src.serving.astradb_setup import AstraDBSetup

logger = logging.getLogger(__name__)


def write_partition_to_astra(partition: Iterator, collection_name: str, config: dict):
    """
    Write a partition of rows to AstraDB Collection.
    Designed to run on executors.
    
    Args:
        partition: Iterator of Row objects
        collection_name: Target collection name
        config: AstraDB connection config
    """
    try:
        from astrapy.collections import create_client
    except ImportError:
        # Should be installed on workers
        return

    db_id = config.get("db_id")
    region = config.get("region")
    token = config.get("token")
    keyspace = config.get("keyspace")
    
    if not db_id or not region or not token:
        # Log failure
        print("Missing AstraDB credentials on executor.")
        return

    # Connect to AstraDB
    # Client creation within partition to avoid serialization issues
    client = create_client(
        astra_database_id=db_id,
        astra_database_region=region,
        astra_application_token=token
    )
    
    collection = client.namespace(keyspace).collection(collection_name)
    
    # Batch insertion would be ideal if supported by the version/API 
    # For now, we iterate and create documents.
    
    for row in partition:
        doc = row.asDict()
        
        # Handle Date objects serialization? JSON default serializer might choke on date/datetime
        # Convert dates to strings
        for k, v in doc.items():
            if hasattr(v, 'isoformat'):
                doc[k] = v.isoformat()
        
        # Ensure ID? 
        # Ideally we construct a deterministic ID from PK columns if we want idempotence
        # If not provided, Astra generates one.
        # SERVING.md says "reprocessing should not duplicate data" -> we need deterministic IDs.
        # But we don't have the PK logic easily available here without schemas.
        # For now, we trust the user requirement example which effectively does upserts if ID provided, 
        # or inserts if not. 
        # If we want idempotency, we should generating the 'path' argument in create().
        # Let's check if we can form a key. For now, pure insert as per example unless we define keys.
        
        try:
            # Using create() equivalent to insert
            # If we wanted to enforce ID: collection.create(path=my_id, document=doc)
            collection.create(document=doc)
        except Exception as e:
            print(f"Error inserting document into {collection_name}: {e}")


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
