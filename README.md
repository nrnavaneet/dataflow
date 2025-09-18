# Billion-Scale Stock Data Generator & Analytics Pipeline

Generate and process **over 1 billion stock trading records** in real time using Kafka, Python, AWS, and more. This project is designed for benchmarking, analytics, and big data engineering.

## Features
- **Generates 1+ billion synthetic stock records** for stress testing and analytics
- Real-time streaming with **Kafka**
- Scalable **Python producer/consumer** scripts
- Efficient storage in **Amazon S3** (Parquet format)
- Automated schema detection and ETL with **AWS Glue**
- Analytics-ready data in **Amazon Redshift**
- Interactive dashboards via **Amazon QuickSight**


## Architecture

1. **Producer**
   - Generates random stock events with fields like `stock`, `price`, `volume`, `exchange`, `order_type`, `trade_id`, etc.
   - Sends messages to Kafka topic `stock-prices` using LZ4 compression and batching.
   - Parallel processes increase throughput.

2. **Consumer**
   - Subscribes to Kafka topic.
   - Buffers messages until `BATCH_SIZE` or `FLUSH_INTERVAL` is reached.
   - Flushes data to **S3** as partitioned Parquet files:
     ```
     s3://your-bucket/stock-prices/year=YYYY/month=MM/day=DD/hour=HH/workerID-part-timestamp.parquet
     ```

3. **AWS Glue Crawler**
   - Scans the S3 bucket.
   - Detects schema and partitions automatically.
   - Populates the **Glue Data Catalog** with metadata for Redshift ETL.

4. **AWS Glue ETL Job**
   - Transforms and cleans stock data.
   - Loads processed data into **Amazon Redshift** for analytics.

5. **Amazon QuickSight**
   - Connects to Redshift to visualize stock metrics.
   - Dashboards can include:
     - Total volume per stock
     - Price trends
     - Sector-wise trading distribution
     - Top traders and ratings
   - Supports near real-time updates as new data arrives.


## Data Schema

| Field            | Type   |
|------------------|--------|
| stock            | string |
| price            | double |
| volume           | long   |
| timestamp        | double |
| exchange         | string |
| order_type       | string |
| trade_id         | string |
| trader           | string |
| sector           | string |
| currency         | string |
| order_source     | string |
| settlement_type  | string |
| rating           | string |

## Getting Started

### Prerequisites
- Docker & docker-compose
- Python 3.8+
- Kafka (local or cloud)
- AWS account (for S3, Glue, Redshift, QuickSight)

### Setup
1. **Clone the repo**
   ```bash
   git clone https://github.com/nrnavaneet/dataflow.git
   cd dataflow
   ```
2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Start Kafka cluster**
   ```bash
   docker-compose up -d
   ```
4. **Run Producer and Consumer**
   ```bash
   python scripts/producer.py
   python scripts/consumer.py
   ```
5. **Or use the provided script**
   ```bash
   ./run.sh
   ```

## Usage

- Configure producer/consumer parameters in `scripts/producer.py` and `scripts/consumer.py`.
- Monitor S3 bucket for Parquet files.
- Use AWS Glue and Redshift for ETL and analytics.
- Build dashboards in QuickSight.

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

MIT
