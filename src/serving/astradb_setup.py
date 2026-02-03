"""
AstraDB Setup Script
Preparing AstraDB to serve marts using Document API (Collections).
Using DataAPIClient.
"""

import os
import logging
from typing import List

try:
    from astrapy.client import DataAPIClient
except ImportError:
    pass

logger = logging.getLogger(__name__)

class AstraDBSetup:
    """AstraDB infrastructure management using Collections"""
    
    KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE", "cloud_analytics")
    
    # Define collections (one per mart)
    COLLECTIONS = [
        "org_daily_usage_by_service",
        "revenue_by_org_month",
        "cost_anomaly_mart",
        "tickets_by_org_date",
        "genai_tokens_by_org_date",
        "org_usage_realtime"
    ]
        
    @classmethod
    def get_client(cls):
        """Create and return an Astra DataAPIClient"""
        token = os.getenv("ASTRA_DB_APPLICATION_TOKEN", os.getenv("ASTRA_TOKEN"))
        
        if not token:
            raise ValueError("Missing AstraDB token (ASTRA_DB_APPLICATION_TOKEN)")
            
        return DataAPIClient(token=token)

    @classmethod
    def get_api_endpoint(cls):
        """Construct API Endpoint from ID and Region"""
        db_id = os.getenv("ASTRA_DB_ID")
        region = os.getenv("ASTRA_DB_REGION")
        
        if not db_id or not region:
            raise ValueError("Missing ASTRA_DB_ID or ASTRA_DB_REGION")
            
        return f"https://{db_id}-{region}.apps.astra.datastax.com"

    @classmethod
    def setup_infrastructure(cls):
        """
        Connect to AstraDB and initialize collections.
        """
        try:
            logger.info("Connecting to AstraDB...")
            client = cls.get_client()
            endpoint = cls.get_api_endpoint()
            
            db = client.get_database(endpoint, keyspace=cls.KEYSPACE)
            logger.info(f"Connected to Database at {endpoint}, namespace: {cls.KEYSPACE}")
            
            for col_name in cls.COLLECTIONS:
                logger.info(f"Initializing collection: {col_name}")
                try:
                    db.create_collection(col_name)
                    logger.info(f"Created collection: {col_name}")
                except Exception as e:
                    if "already exists" in str(e).lower() or "conflict" in str(e).lower():
                        logger.info(f"Collection {col_name} probably already exists.")
                    else:
                        logger.warning(f"Error creating collection {col_name}: {e}")
                
            logger.info("Infrastructure setup completed.")
            
        except Exception as e:
            logger.error(f"Failed to setup AstraDB infrastructure: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    AstraDBSetup.setup_infrastructure()
