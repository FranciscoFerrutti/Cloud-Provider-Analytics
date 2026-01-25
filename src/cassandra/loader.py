"""
Load data from Gold layer to Cassandra/AstraDB
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
import logging

from src.utils.config import Config
from src.cassandra.schema import CassandraSchema

logger = logging.getLogger(__name__)


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
        logger.info("Creating Cassandra keyspace and tables")
        
        try:
            # This would typically use astrapy or cassandra-driver
            # For now, we'll provide the CQL statements
            statements = CassandraSchema.get_all_cql_statements()
            
            logger.info(f"Generated {len(statements)} CQL statements")
            for stmt in statements:
                logger.debug(f"CQL: {stmt[:100]}...")
            
            # In production, execute these via cassandra-driver
            try:
                import os
                from cassandra.cluster import Cluster
                from cassandra.auth import PlainTextAuthProvider
                
                bundle_path = self.cassandra_config.get("secure_bundle_path")
                token = self.cassandra_config.get("token")
                
                if bundle_path and os.path.exists(bundle_path) and token:
                    logger.info(f"Connecting to Cassandra using Bundle: {bundle_path}")
                    cloud_config = {'secure_connect_bundle': bundle_path}
                    auth_provider = PlainTextAuthProvider('token', token)
                    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                    session = cluster.connect()
                    
                    for stmt in statements:
                        try:
                            session.execute(stmt)
                            logger.info(f"Executed: {stmt[:50]}...")
                        except Exception as e:
                            # Log but continue (some objects might already exist)
                            if "already exists" in str(e):
                                logger.info(f"Object already exists: {stmt[:50]}...")
                            else:
                                logger.warning(f"Error executing CQL: {e}")
                                
                    cluster.shutdown()
                    logger.info("CQL execution completed via cassandra-driver")
                else:
                    logger.warning("Secure Connect Bundle not found or Token missing. Skipping CQL execution via driver.")
                    
            except ImportError:
                logger.warning("cassandra-driver not installed. Skipping DDL execution.")
            except Exception as e:
                logger.error(f"Failed to execute CQL via driver: {e}")
            
        except Exception as e:
            logger.error(f"Error creating Cassandra schema: {e}")
            raise
    
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
        
        # Ensure date column is in correct format
        if "date" in df.columns:
            df = df.withColumn("date", to_date(col("date")))
        
        # Write to Cassandra using Spark Cassandra connector
        # Note: Requires spark-cassandra-connector
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(
                    table=table_name,
                    keyspace=CassandraSchema.KEYSPACE,
                    host=self.cassandra_config["host"],
                    port=self.cassandra_config["port"]
                ) \
                .save()
            
            logger.info(f"Successfully loaded {df.count()} records to Cassandra/{table_name}")
            
        except Exception as e:
            logger.error(f"Error loading to Cassandra: {e}")
            logger.info("Falling back to manual insertion via astrapy")
            # Fallback: use astrapy for manual insertion
            self._load_via_astrapy(df, table_name)
    
    def _load_via_astrapy(self, df: DataFrame, table_name: str):
        """
        Load data using astrapy library (fallback method)
        
        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        logger.info(f"Loading via astrapy to {table_name}")
        
        try:
            from astrapy import DataAPIClient
            from astrapy.db import AstraDB
            
            # Initialize AstraDB client
            client = DataAPIClient(token=self.cassandra_config["token"])
            db = client.get_database_by_api_endpoint(
                self.cassandra_config["host"]
            )
            collection = db.get_collection(table_name)
            
            # Convert DataFrame to list of dicts
            records = df.toPandas().to_dict('records')
            
            # Insert in batches
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}")
            
            logger.info(f"Successfully loaded {len(records)} records via astrapy")
            
        except ImportError:
            logger.warning("astrapy not available. Skipping Cassandra load.")
        except Exception as e:
            logger.error(f"Error loading via astrapy: {e}")
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

