"""
Cassandra schema definitions
"""

from typing import Dict

class CassandraSchema:
    """Cassandra schema management"""
    
    KEYSPACE = "cloud_analytics"
    
    @staticmethod
    def get_table_definitions() -> Dict[str, str]:
        """
        Get CQL definitions for all tables
        
        Returns:
            Dictionary of table_name -> create_statement
        """
        tables = {}
        
        # FinOps: Daily usage by org and service
        tables["org_daily_usage_by_service"] = f"""
        CREATE TABLE IF NOT EXISTS {CassandraSchema.KEYSPACE}.org_daily_usage_by_service (
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
        CREATE TABLE IF NOT EXISTS {CassandraSchema.KEYSPACE}.revenue_by_org_month (
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
        CREATE TABLE IF NOT EXISTS {CassandraSchema.KEYSPACE}.cost_anomaly_mart (
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
        CREATE TABLE IF NOT EXISTS {CassandraSchema.KEYSPACE}.tickets_by_org_date (
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
        CREATE TABLE IF NOT EXISTS {CassandraSchema.KEYSPACE}.genai_tokens_by_org_date (
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
    def get_all_cql_statements() -> list[str]:
        """
        Get all CQL statements as a list
        
        Returns:
            List of CQL strings
        """
        return list(CassandraSchema.get_table_definitions().values())
