"""
Anomaly detection using multiple methods: z-score, MAD, and percentiles
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, abs, lit, mean, stddev, percentile_approx,
    collect_list, array, array_min, array_max, size
)
from pyspark.sql.types import DoubleType
import logging

from src.utils.config import Config

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Anomaly detection using multiple statistical methods"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize anomaly detector
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.thresholds = Config.QUALITY_THRESHOLDS
    
    def detect_all_anomalies(self, df: DataFrame, value_col: str = "daily_cost_usd") -> DataFrame:
        """
        Apply all anomaly detection methods using optimized single-pass statistics.
        
        Args:
            df: Input DataFrame
            value_col: Column to analyze
            
        Returns:
            DataFrame with all anomaly detection results
        """
        logger.info(f"Detecting anomalies for {value_col} using optimized execution")
        
        if df.isEmpty():
            logger.warning("No data for anomaly detection")
            return self._create_empty_result(df)

        # 1. Cache data to accelerate multiple statistical computations
        df.cache()
        
        try:
            # 2. Compute global statistics efficiently
            # Pass 1: Mean and StdDev (Z-Score)
            stats = df.select(
                mean(col(value_col)).alias("mean"),
                stddev(col(value_col)).alias("std")
            ).collect()[0]
            
            mean_val = stats["mean"] or 0.0
            std_val = stats["std"] or 1.0
            
            # Pass 2: Quantiles for Median (MAD) and Percentile (P99)
            # 0.5 = Median, 0.99 = 99th Percentile
            quantiles = df.approxQuantile(value_col, [0.5, 0.99], 0.05)
            median_val = quantiles[0]
            p99_val = quantiles[1]
            
            # Pass 3: MAD Calculation (Median Absolute Deviation)
            # Requires calculating abs(value - median) first
            # Since DF is cached, this is fast
            df_with_deviation = df.withColumn("abs_diff", abs(col(value_col) - median_val))
            mad_val = df_with_deviation.approxQuantile("abs_diff", [0.5], 0.05)[0] or 1.0
            
            # 3. Prepare Thresholds
            z_thresh = self.thresholds.get("z_score_threshold", 3.0)
            mad_thresh = self.thresholds.get("mad_threshold", 3.0)
            percentile_factor = self.thresholds.get("anomaly_factor", 1.5)
            p_thresh_val = p99_val * percentile_factor
            
            logger.info(f"Stats: Mean={mean_val:.2f}, Std={std_val:.2f}, Median={median_val:.2f}, MAD={mad_val:.2f}")

            # 4. Apply Vectorized Logic
            result_df = df.withColumn("z_score", (col(value_col) - mean_val) / std_val) \
                          .withColumn("mad_score", (0.6745 * abs(col(value_col) - median_val)) / mad_val) \
                          .withColumn("percentile_threshold", lit(p_thresh_val)) \
                          .withColumn("is_anomaly_zscore", abs(col("z_score")) > z_thresh) \
                          .withColumn("is_anomaly_mad", col("mad_score") > mad_thresh) \
                          .withColumn("is_anomaly_percentile", col(value_col) > p_thresh_val) \
                          .withColumn("is_anomaly", 
                                      col("is_anomaly_zscore") | 
                                      col("is_anomaly_mad") | 
                                      col("is_anomaly_percentile"))
            
            # Materialize to count anomalies (and trigger cache usage)
            anomalies = result_df.filter(col("is_anomaly")).count()
            logger.info(f"Detected {anomalies} anomalies out of {result_df.count()} records")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in anomaly detection: {e}")
            df.unpersist()
            raise e
            
    def _create_empty_result(self, df: DataFrame) -> DataFrame:
        """Helper to return empty result with correct schema"""
        return df.withColumn("is_anomaly", lit(False)) \
                 .withColumn("is_anomaly_zscore", lit(False)) \
                 .withColumn("is_anomaly_mad", lit(False)) \
                 .withColumn("is_anomaly_percentile", lit(False)) \
                 .withColumn("z_score", lit(0.0)) \
                 .withColumn("mad_score", lit(0.0)) \
                 .withColumn("percentile_threshold", lit(0.0))

