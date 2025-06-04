# Canalytics Storage Module

This module provides utilities for storing and loading data from AWS S3 and ClickHouse for the Canalytics project.

## Module Structure

- `s3_loader.py`: Contains the `S3Loader` class for all S3 bucket operations
- `clickhouse_loader.py`: Contains the `ClickHouseLoader` class for ClickHouse database operations
- `db_loader.py`: Contains utility functions that leverage both loaders to process files and directories
- `__init__.py`: Package initialization file that exposes key classes and functions

## Features

- **S3 Operations**:

  - File uploads and downloads
  - JSON data uploads
  - File listing and deletion
  - Automatic content-type detection

- **ClickHouse Operations**:

  - Efficient connection management
  - Table creation with optimized schemas
  - Data loading for both AIS shipping data and news articles
  - High-performance analytical queries

- **Utility Functions**:
  - Automatic data type detection and routing
  - Batch processing of directories
  - Error handling and detailed logging

## Installation and Setup

1. Copy the `env.example` file to `.env` in the project root and fill in your credentials:

```bash
cp env.example .env
# Edit .env with your actual values
```

2. Required environment variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=us-east-1
CANALYTICS_S3_BUCKET=canalytics-data

# ClickHouse Configuration
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=canalytics
CLICKHOUSE_USER=canalytics_user
CLICKHOUSE_PASSWORD=canalytics_password
```

3. Install required Python packages:

```bash
pip install -r requirements.txt
```

4. Create the database tables:

```bash
python -m storage.db_loader --init-db --db
```

## Usage Examples

### Using Loaders Directly

```python
from storage import S3Loader, ClickHouseLoader

# Initialize the S3 loader
s3 = S3Loader(bucket_name="canalytics-data")

# Upload a file to S3
s3.upload_file("path/to/data.json", "data/processed/data.json")

# Initialize the ClickHouse loader
db = ClickHouseLoader(
    host="clickhouse",
    port=8123,
    database="canalytics",
    user="canalytics_user",
    password="canalytics_password"
)
db.connect()

# Create tables if they don't exist
db.create_tables()

# Insert vessel data
db.insert_vessel({
    "mmsi": 123456789,
    "name": "EVER GIVEN",
    "type": "Container Ship",
    "length": 399.94,
    "width": 59.0,
    "draft": 14.5
})

# Query vessel positions
from datetime import datetime, timedelta
now = datetime.now()
positions = db.get_vessel_positions(
    mmsi=123456789,
    start_time=now - timedelta(days=7),
    end_time=now
)

# Don't forget to disconnect
db.disconnect()
```

### Using Utility Functions

```python
from storage import load_file_to_s3_and_db, process_directory
from storage import S3Loader, ClickHouseLoader
import os

# Initialize loaders
s3 = S3Loader()
db = ClickHouseLoader(
    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    database=os.getenv("CLICKHOUSE_DB", "canalytics"),
    user=os.getenv("CLICKHOUSE_USER", "canalytics_user"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "canalytics_password")
)
db.connect()

try:
    # Process a single file
    load_file_to_s3_and_db("data/raw/ais/ais_20250529_082843_558524.json", s3, db)

    # Process an entire directory
    results = process_directory("data/raw/", s3, db)
    print(f"Successfully processed {results['success']} files, {results['failure']} failed")
finally:
    # Always disconnect when done
    db.disconnect()
```

### Command Line Usage

```bash
# Initialize database tables
python -m storage.db_loader --init-db --db

# Process a single file to both S3 and DB
python -m storage.db_loader --s3 --db --file data/raw/ais/ais_20250529_082843_558524.json

# Process all files in a directory
python -m storage.db_loader --s3 --db --dir data/raw/

# Process only to S3 (skip database)
python -m storage.db_loader --s3 --dir data/raw/news/
```

## Infrastructure Setup

For full infrastructure setup instructions including provisioning S3 buckets and ClickHouse instances, refer to the project documentation.
