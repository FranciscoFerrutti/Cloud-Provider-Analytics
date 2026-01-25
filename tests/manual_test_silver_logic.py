"""
Manual verification for Silver Layer Logic (SCD & Schema)
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col
import shutil
import os
import sys

# Add src to path
sys.path.append(os.getcwd())

from src.silver.schema_manager import SchemaManager
from src.silver.scd_manager import SCDManager
from src.utils.config import Config

def get_spark():
    return SparkSession.builder \
        .appName("ManualTest") \
        .master("local[*]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

def test_schema_manager(spark):
    print("\n" + "="*30)
    print("TESTING SCHEMA MANAGER")
    print("="*30)
    
    mgr = SchemaManager(spark)
    
    # Mock V1 Data (timestamp, no new cols)
    v1_data = [(1, "2023-01-01 10:00:00", 100.0)]
    v1_schema = ["event_id", "timestamp", "value"]
    df_v1 = spark.createDataFrame(v1_data, schema=v1_schema)
    
    print("Input V1 Schema:")
    df_v1.printSchema()
    
    df_std = mgr.standardize_schema(df_v1, "usage_events")
    
    print("Standardized Data:")
    df_std.show()
    print("Standardized Schema:")
    df_std.printSchema()
    
    # Assertions
    cols = df_std.columns
    assert "event_ts" in cols, "timestamp should be renamed to event_ts"
    assert "carbon_kg" in cols, "carbon_kg should be added"
    assert "genai_tokens" in cols, "genai_tokens should be added"
    
    row = df_std.collect()[0]
    assert row["event_ts"] is not None
    assert row["carbon_kg"] == 0.0
    print("Schema Manager Test PASSED")

def test_scd_manager(spark):
    print("\n" + "="*30)
    print("TESTING SCD MANAGER")
    print("="*30)
    
    mgr = SCDManager(spark)
    tmp_path = "tmp/scd_test_dimension"
    
    # 1. Initial Load (Day 1)
    # Organization A: Tier 1
    # Organization B: Tier 1
    data_day1 = [("org1", "Org A", "Tier 1"), ("org2", "Org B", "Tier 1")]
    schema = ["org_id", "name", "tier"]
    
    df_day1 = spark.createDataFrame(data_day1, schema)
    
    # Mock Config path (monkeypatch if needed, or just rely on overwrite)
    # Since SCDManager uses Config.get_silver_path, let's mock it or just use a table name that maps safely?
    # Actually, Config might point to a real path. Let's patch get_silver_path temporarily or use a specific table name.
    # Config is a class with static methods, hard to patch without mock lib. 
    # But Config.SILVER_PATH is a var.
    
    original_silver = Config.SILVER_PATH
    Config.SILVER_PATH = "tmp/silver_test"
    
    try:
        # Initial Run
        print("Running Day 1 (Initial Load)...")
        mgr.apply_scd_type_2(df_day1, "test_orgs", keys=["org_id"], scd_cols=["name", "tier"])
        
        # Verify Day 1
        res1 = spark.read.parquet(f"{Config.SILVER_PATH}/test_orgs")
        res1.show()
        assert res1.count() == 2
        assert res1.filter("is_current = true").count() == 2
        
        # 2. Day 2 Update
        # Org A: Changed to Tier 2 (Update)
        # Org B: Unchanged
        # Org C: New Record (Insert)
        
        data_day2 = [("org1", "Org A", "Tier 2"), ("org2", "Org B", "Tier 1"), ("org3", "Org C", "Tier 1")]
        df_day2 = spark.createDataFrame(data_day2, schema)
        
        print("Running Day 2 (Updates)...")
        mgr.apply_scd_type_2(df_day2, "test_orgs", keys=["org_id"], scd_cols=["name", "tier"])
        
        # Verify Day 2
        res2 = spark.read.parquet(f"{Config.SILVER_PATH}/test_orgs")
        res2.orderBy("org_id", "start_date").show()
        
        # Checks
        # Total records should be:
        # Org 1: 1 Old (Closed), 1 New (Open) -> 2
        # Org 2: 1 Old (Open) -> 1
        # Org 3: 1 New (Open) -> 1
        # Total: 4
        
        count = res2.count()
        print(f"Total Count: {count}")
        assert count == 4
        
        # Org 1 Check
        org1 = res2.filter("org_id = 'org1'").orderBy("start_date").collect()
        assert len(org1) == 2
        assert org1[0]["is_current"] == False
        assert org1[0]["end_date"] is not None
        assert org1[1]["is_current"] == True
        assert org1[1]["tier"] == "Tier 2"
        
        # Org 2 Check
        org2 = res2.filter("org_id = 'org2'").collect()
        assert len(org2) == 1
        assert org2[0]["tier"] == "Tier 1"
        assert org2[0]["is_current"] == True
        
        print("SCD Manager Test PASSED")
        
    finally:
        # Cleanup
        Config.SILVER_PATH = original_silver
        if os.path.exists("tmp"):
            shutil.rmtree("tmp")

if __name__ == "__main__":
    spark = get_spark()
    test_schema_manager(spark)
    test_scd_manager(spark)
    print("\nALL TESTS PASSED")
