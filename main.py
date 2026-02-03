"""
Main entry point for Cloud Provider Analytics Pipeline

Usage:
    python main.py --layer batch
    python main.py --layer silver
    python main.py --layer gold
    python main.py --layer serving
    python main.py --full
    python main.py --streaming
"""

import argparse
import sys
import logging
from src.pipeline import Pipeline
from src.utils.logger import setup_logging
from src.utils.spark_utils import create_spark_session

logger = logging.getLogger(__name__)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Cloud Provider Analytics Pipeline")
    parser.add_argument(
        "--layer",
        choices=["batch", "silver", "gold", "serving"],
        help="Run a specific layer"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full pipeline (batch + silver + gold + serving)"
    )
    parser.add_argument(
        "--etl-only",
        action="store_true",
        help="Run ETL pipeline (batch + silver + gold) without serving"
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Start streaming (Speed Layer)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(log_level=args.log_level)
    
    # Create Spark session
    spark = create_spark_session("CloudProviderAnalytics")
    
    # Initialize pipeline
    pipeline = Pipeline(spark)
    
    try:
        if args.full:
            # Run full pipeline
            pipeline.run_full_pipeline()
            
        elif args.etl_only:
            # Run ETL only (Batch -> Silver -> Gold)
            pipeline.run_batch_layer()
            pipeline.run_silver_layer()
            pipeline.run_gold_layer()
            logger.info("ETL Pipeline (Batch->Silver->Gold) Completed")
        
        elif args.streaming:
            # Start streaming
            # Start streaming
            queries = pipeline.run_speed_layer()
            print("\n" + "="*60)
            print("Streaming queries started successfully!")
            print(f"Active streams: {len(spark.streams.active)}")
            print("\nTo stop the query, press Ctrl+C")
            print("Waiting for any query to terminate...")
            print("="*60 + "\n")
            
            # Wait for termination
            spark.streams.awaitAnyTermination()
        
        elif args.layer == "batch":
            pipeline.run_batch_layer()
        
        elif args.layer == "silver":
            pipeline.run_silver_layer()
        
        elif args.layer == "gold":
            pipeline.run_gold_layer()
        
        elif args.layer == "serving":
            pipeline.run_serving_layer()
        
        else:
            parser.print_help()
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        sys.exit(0)
    
    except Exception as e:
        print(f"\nError running pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

