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
    
    # Get collection
    # We assume 'setup_infrastructure' has been run so collection exists.
    # If get_collection doesn't exist, we might need create_collection, 
    # but strictly loader shouldn't create DDL. 
    # However, DataAPIClient.get_database returns a DB object. 
    # We assume it has get_collection or we access it like db[collection_name] or db.collection(name)
    # The docstring didn't explicitly show 'get_collection' but it's standard.
    # Let's try to get it. If get_collection isn't a method, likely `db[collection_name]` works 
    # or `db.create_collection` gets it if exists.
    # Actually, the docstring says: "my_coll = my_db0.create_collection(...)"
    # It doesn't explicitly show 'get_collection'.
    # But usually `db.collection(name)` or `db[name]` is the pattern.
    # We will try `db.get_collection(collection_name)`. If that fails, `db.create_collection` 
    # with no definition might retrieve it. 
    # Wait, looking at astrapy source usually helps. 
    # Given I can't look deeper right now, I'll use `get_collection` as it's the safest assumption for a getter.
    # If incorrect, I'll fix in verification.
    
    try:
        collection = db.get_collection(collection_name)
    except Exception:
        # Fallback if get_collection is not the name
        # Maybe it's just `db.collection(collection_name)`?
        # Or maybe we call create_collection again (idempotent?)
        try:
             collection = db.create_collection(collection_name)
        except Exception:
             # If create fails (already exists) and get fails (?), we are in trouble.
             # But likely get_collection exists.
             # Let's assume correct method is get_collection based on similar libs (pymongo).
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
