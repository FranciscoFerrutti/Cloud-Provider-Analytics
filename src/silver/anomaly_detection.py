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
    
    def detect_anomalies_zscore(self, df: DataFrame, value_col: str = "daily_cost_usd") -> DataFrame:
        """
        Detect anomalies using z-score method
        
        Args:
            df: Input DataFrame
            value_col: Column to analyze
            
        Returns:
            DataFrame with z-score anomaly flags
        """
        logger.info(f"Detecting anomalies using z-score for {value_col}")
        
        # Calculate mean and stddev
        stats = df.select(
            mean(col(value_col)).alias("mean_val"),
            stddev(col(value_col)).alias("stddev_val")
        ).collect()[0]
        
        mean_val = stats["mean_val"] or 0.0
        stddev_val = stats["stddev_val"] or 1.0
        
        # Calculate z-score
        df = df.withColumn(
            "z_score",
            (col(value_col) - mean_val) / stddev_val
        )
        
        # Flag anomalies
        df = df.withColumn(
            "is_anomaly_zscore",
            abs(col("z_score")) > self.thresholds["z_score_threshold"]
        )
        
        return df
    
    def detect_anomalies_mad(self, df: DataFrame, value_col: str = "daily_cost_usd") -> DataFrame:
        """
        Detect anomalies using Median Absolute Deviation (MAD) method
        
        Args:
            df: Input DataFrame
            value_col: Column to analyze
            
        Returns:
            DataFrame with MAD anomaly flags
        """
        logger.info(f"Detecting anomalies using MAD for {value_col}")
        
        # Calculate median
        median = df.approxQuantile(value_col, [0.5], 0.25)[0]
        
        # Calculate absolute deviations from median
        df = df.withColumn(
            "abs_deviation",
            abs(col(value_col) - median)
        )
        
        # Calculate MAD (median of absolute deviations)
        mad = df.approxQuantile("abs_deviation", [0.5], 0.25)[0] or 1.0
        
        # Calculate modified z-score
        df = df.withColumn(
            "mad_score",
            0.6745 * col("abs_deviation") / mad
        )
        
        # Flag anomalies
        df = df.withColumn(
            "is_anomaly_mad",
            col("mad_score") > self.thresholds["mad_threshold"]
        )
        
        return df.drop("abs_deviation")
    
    def detect_anomalies_percentile(self, df: DataFrame, value_col: str = "daily_cost_usd") -> DataFrame:
        """
        Detect anomalies using percentile method
        
        Args:
            df: Input DataFrame
            value_col: Column to analyze
            
        Returns:
            DataFrame with percentile anomaly flags
        """
        logger.info(f"Detecting anomalies using percentiles for {value_col}")
        
        # Calculate percentiles
        percentiles = df.approxQuantile(
            value_col,
            [0.01, 0.99],
            0.25
        )
        
        p1 = percentiles[0] if percentiles[0] else 0.0
        p99 = percentiles[1] if percentiles[1] else 0.0
        
        # Calculate threshold
        threshold = p99 * self.thresholds["anomaly_factor"]
        
        # Flag anomalies (above p99 * factor)
        df = df.withColumn(
            "is_anomaly_percentile",
            col(value_col) > threshold
        )
        
        df = df.withColumn(
            "percentile_threshold",
            lit(threshold)
        )
        
        return df
    
    def detect_all_anomalies(self, df: DataFrame, value_col: str = "daily_cost_usd") -> DataFrame:
        """
        Apply all anomaly detection methods and create combined flag
        
        Args:
            df: Input DataFrame
            value_col: Column to analyze
            
        Returns:
            DataFrame with all anomaly detection results
        """
        logger.info("Applying all anomaly detection methods")
        
        # Apply all methods
        df = self.detect_anomalies_zscore(df, value_col)
        df = self.detect_anomalies_mad(df, value_col)
        df = self.detect_anomalies_percentile(df, value_col)
        
        # Combined anomaly flag (anomaly if detected by any method)
        df = df.withColumn(
            "is_anomaly",
            (col("is_anomaly_zscore") | col("is_anomaly_mad") | col("is_anomaly_percentile"))
        )
        
        # Count anomalies
        anomaly_count = df.filter(col("is_anomaly") == True).count()
        logger.info(f"Detected {anomaly_count} anomalies out of {df.count()} records")
        
        return df

