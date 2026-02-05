"""
Configuration management for the pipeline
"""

import os
from typing import Dict, Any
from pathlib import Path


class Config:
    """Pipeline configuration"""
    
    # Data Lake paths (use /datalake in Docker, configurable via env)
    BASE_PATH = os.getenv("DATALAKE_BASE_PATH", "/datalake")
    LANDING_PATH = os.path.join(BASE_PATH, "landing")
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
    SILVER_PATH = os.path.join(BASE_PATH, "silver")
    GOLD_PATH = os.path.join(BASE_PATH, "gold")
    QUARANTINE_PATH = os.path.join(BASE_PATH, "quarantine")
    
    # Checkpoint paths (use /tmp in Docker, configurable via env)
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/checkpoints")
    STREAMING_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "streaming")
    
    # Data source paths
    LANDING_SOURCES = {
        "customers_orgs": os.path.join(LANDING_PATH, "customers_orgs.csv"),
        "users": os.path.join(LANDING_PATH, "users.csv"),
        "resources": os.path.join(LANDING_PATH, "resources.csv"),
        "support_tickets": os.path.join(LANDING_PATH, "support_tickets.csv"),
        "marketing_touches": os.path.join(LANDING_PATH, "marketing_touches.csv"),
        "nps_surveys": os.path.join(LANDING_PATH, "nps_surveys.csv"),
        "billing_monthly": os.path.join(LANDING_PATH, "billing_monthly.csv"),
        "usage_events_stream": os.path.join(LANDING_PATH, "usage_events_stream")
    }
    
    # Streaming configuration
    STREAMING_CONFIG = {
        "trigger_interval": "30 seconds",
        "max_files_per_trigger": 10,
        "watermark_delay": "10 minutes",
        "window_duration": "1 hour",
        "window_slide": "30 minutes",
        "batch_timeout_seconds": 60
    }
    
    # Data quality thresholds
    QUALITY_THRESHOLDS = {
        "min_cost_increment": -0.01,
        "anomaly_factor": 1.5,  # p99 * factor
        "z_score_threshold": 3.0,
        "mad_threshold": 3.0,
        "percentile_threshold": 99
    }
    
    # Cassandra/AstraDB configuration
    CASSANDRA_CONFIG = {
        "db_id": os.getenv("ASTRA_DB_ID", ""),
        "region": os.getenv("ASTRA_DB_REGION", ""),
        "token": os.getenv("ASTRA_DB_APPLICATION_TOKEN", os.getenv("ASTRA_TOKEN", "")),
        "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "cloud_analytics"),
        # Keep legacy locally just in case, but prioritize above
    }
    
    # Partition columns
    PARTITION_COLUMNS = {
        "bronze": ["year", "month", "day"],
        "silver": ["year", "month", "day", "service"],
        "gold": ["year", "month", "day"]
    }
    
    @classmethod
    def get_bronze_path(cls, table_name: str) -> str:
        """Get bronze table path"""
        return os.path.join(cls.BRONZE_PATH, table_name)
    
    @classmethod
    def get_silver_path(cls, table_name: str) -> str:
        """Get silver table path"""
        return os.path.join(cls.SILVER_PATH, table_name)
    
    @classmethod
    def get_gold_path(cls, table_name: str) -> str:
        """Get gold table path"""
        return os.path.join(cls.GOLD_PATH, table_name)
    
    @classmethod
    def get_checkpoint_path(cls, table_name: str) -> str:
        """Get checkpoint path"""
        return os.path.join(cls.STREAMING_CHECKPOINT, table_name)
    
    @classmethod
    def get_quarantine_path(cls, layer: str, table_name: str) -> str:
        """Get quarantine path for invalid records"""
        return os.path.join(cls.QUARANTINE_PATH, layer, table_name)

