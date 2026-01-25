# Cloud Provider Analytics - Lambda Architecture Pipeline

A comprehensive Big Data analytics pipeline for a Cloud Provider, implementing a Lambda Architecture with PySpark, Structured Streaming, and Cassandra/AstraDB.

## Architecture Overview

This project implements a **Lambda Architecture** with three main layers:

### Batch Layer
- Processes historical data from CSV files
- Master data ingestion (customers, users, resources, tickets, billing, NPS, marketing)
- Full historical reprocessing capability

### Speed Layer
- Real-time processing using Spark Structured Streaming
- Handles `usage_events_stream` JSONL files
- Windowed aggregations with watermarks
- Deduplication by `event_id`
- Late data handling

### Serving Layer
- Business marts published to Cassandra/AstraDB
- Optimized for analytical queries
- Query-driven data modeling

## Data Lake Zones

### Landing (Raw)
- Immutable source data
- CSV and JSONL files
- No transformations applied

### Bronze (Raw Standardized)
- Type standardization
- Audit fields (`ingest_ts`, `source_file`)
- Deduplication
- Quarantine for invalid records
- Partitioned by date

### Silver (Conformed)
- Data cleaning and normalization
- Null handling and outlier treatment
- Schema version compatibility (v1/v2)
- Enrichment with master data
- Feature engineering
- Anomaly detection (z-score, MAD, percentiles)
- Partitioned by date and service

### Gold (Business Marts)
- Analytics-ready tables
- Aggregated metrics
- Partitioned by date
- Ready for BI consumption

## Project Structure

```
project-root/
│
├── src/
│   ├── ingestion/          # Batch and streaming ingestion
│   ├── quality/             # Data quality validation
│   ├── bronze/              # Bronze layer (placeholder)
│   ├── silver/              # Silver transformations and anomaly detection
│   ├── gold/                # Gold marts creation
│   ├── streaming/           # Structured Streaming (Speed Layer)
│   ├── cassandra/           # Cassandra/AstraDB integration
│   ├── utils/               # Utilities and configuration
│   └── pipeline.py          # Main pipeline orchestration
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── notebooks/
│   ├── exploration/         # Data exploration
│   ├── streaming_examples/   # Streaming demos
│   └── business_queries/     # Analytics queries
│
├── config/                   # Configuration files
├── docs/                     # Documentation
└── README.md
```

## Features

### Data Quality
- Validation rules for usage events
- Schema version compatibility (v1/v2)
- Quarantine management for invalid records
- Deduplication by `event_id`

### Anomaly Detection
- **Z-Score**: Statistical outlier detection
- **MAD (Median Absolute Deviation)**: Robust outlier detection
- **Percentiles**: Threshold-based anomaly detection
- Combined anomaly flag

### Gold Marts
1. **FinOps**:
   - `org_daily_usage_by_service`: Daily usage metrics
   - `revenue_by_org_month`: Monthly revenue aggregation
   - `cost_anomaly_mart`: Detected cost anomalies

2. **Support**:
   - `tickets_by_org_date`: Ticket metrics, SLA breach rates, CSAT scores

3. **Product/GenAI**:
   - `genai_tokens_by_org_date`: GenAI usage and cost metrics

## Installation

### Prerequisites
- Docker Desktop (recommended for Windows)
- OR Python 3.8+ with PySpark 3.x (Linux/Mac/Colab)
- AstraDB account (for Cassandra serving layer)

### Setup with Docker (Recommended for Windows)

1. Build the Docker image:
```bash
docker build -t cloud-provider-analytics .
```

2. Prepare data directories:
```bash
mkdir -p datalake/{landing,bronze,silver,gold,quarantine}
mkdir -p checkpoints
```

3. Place your data files in `landing/` directory

4. Set environment variables in `docker-compose.yml` or use `-e` flags

See [docker-commands.md](docker-commands.md) for detailed Docker usage.

### Setup without Docker (Linux/Mac/Colab)

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
export DATALAKE_BASE_PATH="/datalake"
export ASTRA_HOST="your-astra-endpoint"
export ASTRA_TOKEN="your-astra-token"
```

3. Copy and configure:
```bash
cp config/config.yaml.example config/config.yaml
# Edit config.yaml with your settings
```

## Usage

### Using Docker (Recommended)

```bash
# Full pipeline
docker-compose run --rm spark-pipeline python main.py --full

# Specific layer
docker-compose run --rm spark-pipeline python main.py --layer batch

# Streaming
docker-compose run --rm spark-pipeline python main.py --streaming
```

See [docker-commands.md](docker-commands.md) for complete Docker usage guide.

### Running Directly (Python)

```python
from src.pipeline import Pipeline
from src.utils.logger import setup_logging

# Setup logging
setup_logging()

# Initialize and run pipeline
pipeline = Pipeline()
pipeline.run_full_pipeline()
```

### Running Individual Layers

```python
# Batch Layer
pipeline.run_batch_layer()

# Silver Layer
pipeline.run_silver_layer()

# Gold Layer
pipeline.run_gold_layer()

# Serving Layer
pipeline.run_serving_layer()
```

### Starting Streaming (Speed Layer)

```python
# Start streaming ingestion
query = pipeline.run_speed_layer()

# Monitor streaming
print(query.status)

# Stop streaming (when needed)
query.stop()
```

### Using Notebooks

1. **Data Exploration**: `notebooks/exploration/data_exploration.ipynb`
2. **Streaming Demo**: `notebooks/streaming_examples/streaming_demo.ipynb`
3. **Business Queries**: `notebooks/business_queries/analytics_queries.ipynb`

## Configuration

### Data Lake Paths
- Configured via `Config` class in `src/utils/config.py`
- Default base path: `/datalake`
- Can be overridden via environment variables

### Streaming Configuration
- Trigger interval: 30 seconds
- Max files per trigger: 10
- Watermark delay: 10 minutes
- Window duration: 1 hour
- Window slide: 30 minutes

### Quality Thresholds
- Minimum cost increment: -0.01
- Anomaly factor: 1.5 (p99 * factor)
- Z-score threshold: 3.0
- MAD threshold: 3.0

## Cassandra Schema Design

### Partition Keys
- Designed for query patterns
- Optimized for time-series queries
- Supports range queries on clustering keys

### Example Queries

```sql
-- Get daily usage for an organization
SELECT * FROM org_daily_usage_by_service 
WHERE org_id = 'org_123' AND date = '2024-01-15';

-- Get monthly revenue
SELECT * FROM revenue_by_org_month 
WHERE org_id = 'org_123' AND year = 2024 AND month = 1;

-- Get cost anomalies
SELECT * FROM cost_anomaly_mart 
WHERE org_id = 'org_123' AND date >= '2024-01-01';
```

## Architecture Decisions

### Lambda Architecture Justification
- **Batch Layer**: Handles historical data reprocessing and master data
- **Speed Layer**: Provides near real-time analytics for operational metrics
- **Serving Layer**: Optimized for query performance in Cassandra

### Data Lake Zones
- **Landing**: Immutable raw data for audit and replay
- **Bronze**: Standardized raw data with audit fields
- **Silver**: Cleaned and enriched data ready for analytics
- **Gold**: Business marts optimized for specific use cases

### Anomaly Detection Methods
- **Z-Score**: Standard statistical method, sensitive to outliers
- **MAD**: Robust to outliers, better for skewed distributions
- **Percentiles**: Business-friendly threshold-based approach
- **Combined**: Flags anomalies detected by any method

### Cassandra Modeling
- Query-driven design
- Partition keys based on access patterns
- Clustering keys for time-series queries
- Denormalized for read performance

## Assumptions

1. **Data Sources**: CSV files are well-formed and available in Landing
2. **Streaming**: Usage events arrive as JSONL micro-batches
3. **Schema Evolution**: Compatible schema versions (v1/v2)
4. **Execution**: Google Colab environment with persistent storage
5. **AstraDB**: Valid credentials and endpoint available
6. **SLA**: Near real-time (30s-1min) is acceptable, not strict <5s

## Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| High latency in streaming without broker | Medium | High | Use micro-batches with maxFilesPerTrigger, efficient partitioning |
| Colab instability (RAM/time limits) | Medium | High | Persist Parquet and checkpoints to external storage, allow job resumption |
| Late data inconsistencies | Medium | Medium | Define watermarks (10 min), preserve late events in quarantine |
| Schema version errors | High | Medium | Validate schema_version in Bronze, send incompatible to quarantine |
| AstraDB write overhead | Medium | Medium | Batch inserts, use Spark connector or astrapy with batch_size |
| Network errors to Astra | Low | Medium | Implement retries, idempotent writes |

## Limitations

1. **Streaming**: Uses file-based streaming (not Kafka/Pub/Sub)
2. **Colab**: Limited by Colab's runtime and memory constraints
3. **Cassandra**: Single keyspace, simple replication strategy
4. **Testing**: Limited unit tests (focus on business logic, not Spark internals)
5. **Monitoring**: Basic logging, no advanced monitoring/metrics

## Future Improvements

1. **Streaming**: Migrate to Kafka/Pub/Sub for true real-time
2. **Monitoring**: Add Prometheus/Grafana for metrics
3. **Schema Registry**: Implement proper schema versioning
4. **Data Lineage**: Track data flow and transformations
5. **CI/CD**: Automated testing and deployment
6. **Performance**: Optimize Spark configurations, caching strategies
7. **Security**: Add encryption, access controls
8. **Documentation**: API documentation, data dictionary

## Testing

Run tests (where applicable):
```bash
pytest tests/
```

Note: Tests focus on business logic, not Spark internals.

## Contributing

1. Follow the project structure
2. Document code in English
3. Maintain separation of concerns
4. Add logging for debugging
5. Update README for significant changes

## License

[Specify license]

## Contact

[Contact information]

