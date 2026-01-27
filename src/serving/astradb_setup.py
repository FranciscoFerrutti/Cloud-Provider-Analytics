"""
AstraDB Setup Script
Preparing AstraDB to serve marts.
"""

import os
import logging
from typing import Dict, List
try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    pass

logger = logging.getLogger(__name__)

class AstraDBSetup:
    """AstraDB infrastructure management"""
    
    KEYSPACE = os.getenv("ASTRA_KEYSPACE_NAME", "cloud_analytics")
    
    @staticmethod
    def get_table_definitions() -> Dict[str, str]:
        """
        Get CQL definitions for all tables
        
        Returns:
            Dictionary of table_name -> create_statement
        """
        tables = {}
        keyspace = AstraDBSetup.KEYSPACE
        
        # FinOps: Daily usage by org and service
        tables["org_daily_usage_by_service"] = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.org_daily_usage_by_service (
            org_id text,
            date date,
            year int,
            month int,
            day int,
            service text,
            region text,
            total_cost_usd double,
            total_requests bigint,
            total_cpu_hours double,
            total_storage_gb_hours double,
            total_genai_tokens bigint,
            total_carbon_kg double,
            PRIMARY KEY ((org_id, date), service, region)
        ) WITH CLUSTERING ORDER BY (service ASC, region ASC);
        """
        
        # FinOps: Monthly revenue by org (Updated with Billing fields)
        tables["revenue_by_org_month"] = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.revenue_by_org_month (
            org_id text,
            year int,
            month int,
            subtotal_usd double,
            credits_usd double,
            taxes_usd double,
            total_revenue_usd double,
            exchange_rate_used double,
            monthly_requests bigint,
            monthly_cpu_hours double,
            monthly_storage_gb_hours double,
            monthly_genai_tokens bigint,
            monthly_carbon_kg double,
            active_days bigint,
            month_str text,
            PRIMARY KEY ((org_id, year), month)
        ) WITH CLUSTERING ORDER BY (month DESC);
        """
        
        # FinOps: Cost anomalies
        tables["cost_anomaly_mart"] = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.cost_anomaly_mart (
            org_id text,
            date date,
            year int,
            month int,
            day int,
            service text,
            region text,
            daily_cost_usd double,
            is_anomaly_zscore boolean,
            is_anomaly_mad boolean,
            is_anomaly_percentile boolean,
            z_score double,
            mad_score double,
            percentile_threshold double,
            PRIMARY KEY ((org_id, date), service)
        ) WITH CLUSTERING ORDER BY (service ASC);
        """
        
        # Support: Tickets by org and date (Updated with critical tickets)
        tables["tickets_by_org_date"] = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.tickets_by_org_date (
            org_id text,
            date date,
            year int,
            month int,
            day int,
            ticket_count bigint,
            avg_csat_score double,
            sla_breach_count bigint,
            sla_breach_rate double,
            avg_resolution_hours double,
            critical_tickets_count bigint,
            PRIMARY KEY ((org_id, date))
        );
        """
        
        # Product: GenAI tokens by org and date (Updated: Removed service from PK)
        tables["genai_tokens_by_org_date"] = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.genai_tokens_by_org_date (
            org_id text,
            date date,
            year int,
            month int,
            day int,
            total_tokens bigint,
            genai_cost_usd double,
            genai_requests bigint,
            PRIMARY KEY ((org_id, date))
        );
        """
        
        return tables

    @staticmethod
    def get_all_cql_statements() -> List[str]:
        """
        Get all CQL statements as a list
        
        Returns:
            List of CQL strings
        """
        return list(AstraDBSetup.get_table_definitions().values())
        
    @classmethod
    def setup_infrastructure(cls):
        """
        Connect to AstraDB and create keyspace and tables.
        Uses environment variables for connection.
        """
        try:
            bundle_path = os.getenv("ASTRA_SECURE_BUNDLE_PATH")
            token = os.getenv("ASTRA_TOKEN")
            
            if not bundle_path or not token:
                logger.warning("ASTRA_SECURE_BUNDLE_PATH or ASTRA_TOKEN not set. Skipping setup.")
                return

            logger.info("Connecting to AstraDB...")
            cloud_config = {'secure_connect_bundle': bundle_path}
            auth_provider = PlainTextAuthProvider('token', token)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            
            # Create Keyspace (if not exists) is usually handled by AstraDB UI/API, 
            # but we can try to create if we are local or have permissions. 
            # Note: AstraDB serverless usually has 1 keyspace pre-created. 
            # We will focus on tables.
            
            # Select keyspace
            try:
                session.set_keyspace(cls.KEYSPACE)
            except Exception:
                logger.warning(f"Keyspace {cls.KEYSPACE} might not exist. Attempting to create (may fail on AstraDB Serverless).")
                session.execute(f"CREATE KEYSPACE IF NOT EXISTS {cls.KEYSPACE} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
                session.set_keyspace(cls.KEYSPACE)

            statements = cls.get_all_cql_statements()
            logger.info(f"Executing {len(statements)} Schema statements...")
            
            for stmt in statements:
                try:
                    session.execute(stmt)
                    logger.info(f"Executed: {stmt.strip().splitlines()[0]}...")
                except Exception as e:
                     logger.error(f"Error executing statement: {e}")
            
            cluster.shutdown()
            logger.info("Infrastructure setup completed.")
            
        except Exception as e:
            logger.error(f"Failed to setup AstraDB infrastructure: {e}")
            raise

# For backward compatibility during refactor if needed, though we preferred updating references.
# CassandraSchema = AstraDBSetup 

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    AstraDBSetup.setup_infrastructure()
