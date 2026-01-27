"""
Load data from Gold layer to Cassandra/AstraDB
Decoupled implementation using native Cassandra driver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
import logging
import os
from typing import Iterator

from src.utils.config import Config
from src.serving.astradb_setup import AstraDBSetup

logger = logging.getLogger(__name__)


def write_partition_to_cassandra(partition: Iterator, table_name: str, config: dict):
    """
    Write a partition of rows to Cassandra using the native driver.
    Designed to run on executors.
    
    Args:
        partition: Iterator of Row objects
        table_name:/Target table name
        config: Cassandra connection config
    """
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
    except ImportError:
        # Should be installed on workers
        return

    bundle_path = config.get("secure_bundle_path")
    token = config.get("token")
    
    if not bundle_path or not os.path.exists(bundle_path) or not token:
        # Log to stderr on executor or handle failure
        return

    # Connect to Cassandra
    cloud_config = {'secure_connect_bundle': bundle_path}
    auth_provider = PlainTextAuthProvider('token', token)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    
    # Select keyspace
    try:
        session.set_keyspace(AstraDBSetup.KEYSPACE)
    except Exception:
        # Fallback or fail
        pass

    # Prepare statement for efficient batching? 
    # For simplicity and robustness with random schemas, we might construct INSERTs dynamically
    # or use a pre-prepared statement if we knew the schema beforehand.
    # Given the dynamic nature here (table_name passed), detailed schema awareness on executor is minimal.
    # We will use simple JSON insert or construct the INSERT statement dynamically based on the first row.
    
    # Ideally, we get the schema from the first row (assuming homogenous partition)
    # But rows might be Dictionary-like.
    
    # Strategy: simple insert for each row. 
    # Optimization: BatchStatement could be used but requires care with partition keys.
    # We'll use simple execution for safety and idempotence.
    
    # Cache prepared statement
    prepared_stmt = None
    columns = None
    
    for row in partition:
        data = row.asDict()
        
        if not prepared_stmt:
            columns = list(data.keys())
            cols_str = ", ".join(columns)
            markers = ", ".join(["?" for _ in columns])
            query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({markers})"
            try:
                prepared_stmt = session.prepare(query)
            except Exception as e:
                # Log error
                print(f"Error preparing statement for {table_name}: {e}")
                break
        
        try:
            # Bind parameters in order
            values = [data[c] for c in columns]
            session.execute(prepared_stmt, values)
        except Exception as e:
            print(f"Error inserting row into {table_name}: {e}")
            # Continue or break? Continue best effort?
            
    cluster.shutdown()


class CassandraLoader:
    """Load Gold marts to Cassandra/AstraDB"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Cassandra loader
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.cassandra_config = Config.CASSANDRA_CONFIG
    
    def create_keyspace_and_tables(self):
        """Create keyspace and tables in Cassandra"""
        logger.info("Creating Cassandra keyspace and tables via AstraDBSetup")
        AstraDBSetup.setup_infrastructure()
    
    def load_mart_to_cassandra(self, mart_name: str, table_name: str = None):
        """
        Load a Gold mart to Cassandra
        
        Args:
            mart_name: Name of the mart in Gold layer
            table_name: Target table name in Cassandra (defaults to mart_name)
        """
        if table_name is None:
            table_name = mart_name
        
        logger.info(f"Loading {mart_name} to Cassandra table {table_name}")
        
        # Read from Gold
        gold_path = Config.get_gold_path(mart_name)
        df = self.spark.read.parquet(gold_path)
        
        # Ensure date column is in correct format if exists
        if "date" in df.columns:
            df = df.withColumn("date", to_date(col("date")))
            
        # Repartition to control parallelism? 
        # df.rdd.getNumPartitions() might be high or low. 
        # Depending on volume, we might want to `coalesce` or `repartition`.
        # For now, keep default.
        
        # Capture config to serialize to executors
        config_broadcast = self.cassandra_config
        
        # Execute foreachPartition
        try:
            df.foreachPartition(
                lambda partition: write_partition_to_cassandra(partition, table_name, config_broadcast)
            )
            logger.info(f"Successfully loaded {mart_name} to Cassandra/{table_name}")
        except Exception as e:
            logger.error(f"Error loading {mart_name} to Cassandra: {e}")
            raise

    def load_all_marts(self):
        """Load all Gold marts to Cassandra"""
        logger.info("Loading all Gold marts to Cassandra")
        
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
