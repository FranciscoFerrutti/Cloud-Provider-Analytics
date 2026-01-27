"""
AstraDB Setup Script
Preparing AstraDB to serve marts using Document API (Collections).
"""

import os
import logging
from typing import List

try:
    from astrapy.collections import create_client, AstraCollection
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
        "genai_tokens_by_org_date"
    ]
        
    @classmethod
    def get_client(cls):
        """Create and return an Astra Client"""
        db_id = os.getenv("ASTRA_DB_ID")
        region = os.getenv("ASTRA_DB_REGION")
        token = os.getenv("ASTRA_DB_APPLICATION_TOKEN", os.getenv("ASTRA_TOKEN"))
        
        if not db_id or not region or not token:
            raise ValueError("Missing AstraDB credentials (ASTRA_DB_ID, ASTRA_DB_REGION, ASTRA_DB_APPLICATION_TOKEN)")
            
        return create_client(
            astra_database_id=db_id,
            astra_database_region=region,
            astra_application_token=token
        )

    @classmethod
    def setup_infrastructure(cls):
        """
        Connect to AstraDB and initialize collections.
        """
        try:
            logger.info("Connecting to AstraDB...")
            client = cls.get_client()
            namespace = client.namespace(cls.KEYSPACE)
            
            logger.info(f"Using namespace: {cls.KEYSPACE}")
            
            # List existing collections? 
            # In Document API, collections are often created on demand or we can explicitly create them.
            # astrapy 0.3.3 might not have a simple 'collection_exists' check without listing.
            # We'll just define them. 
            
            for col_name in cls.COLLECTIONS:
                logger.info(f"Initializing collection: {col_name}")
                collection = namespace.collection(col_name)
                # We could try a dummy operation or just assume it's ready.
                # If we wanted to ensure creation, we might insert/delete a dummy doc, 
                # but usually getting the collection object is enough for the client-side reference.
                # True 'creation' happens on first write or explicit create call if API supports it.
                
            logger.info("Infrastructure setup completed.")
            
        except Exception as e:
            logger.error(f"Failed to setup AstraDB infrastructure: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    AstraDBSetup.setup_infrastructure()
