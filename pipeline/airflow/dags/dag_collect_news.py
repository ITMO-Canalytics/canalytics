"""
DAG for collecting AIS and news data for Canalytics project.

This DAG runs two tasks:
1. Collect AIS data from the AIS stream
2. Collect news data from the news API
3. Process collected data to S3 and ClickHouse
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add parent directory to path so we can import modules
sys.path.insert(0, "/opt/airflow")

# Default arguments for the DAG
default_args = {
    "owner": "canalytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "canalytics_collect_news_data",
    default_args=default_args,
    description="Collect news data for Canalytics",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 30),
    catchup=False,
    tags=["canalytics", "data_collection"],
)


# Task to collect news data
def collect_news_data():
    """Run the news collector to fetch maritime news articles."""
    from collectors.news_collector import NewsCollector

    # Create output directory if it doesn't exist
    os.makedirs("/opt/airflow/data/raw/news", exist_ok=True)

    # Run the collector
    collector = NewsCollector()
    collector.collect()
    return True


# Task to process raw data
def process_raw_data():
    """Process raw data files and load to S3 and ClickHouse."""
    from storage import S3Loader, ClickHouseLoader, process_directory

    # Initialize loaders
    s3_loader = None
    db_loader = None

    try:
        s3_loader = S3Loader()
        print("S3 Loader initialized")
    except Exception as e:
        print(f"Failed to initialize S3 loader: {str(e)}")

    try:
        db_loader = ClickHouseLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "canalytics"),
            user=os.getenv("CLICKHOUSE_USER", "canalytics_user"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "canalytics_password"),
        )
        db_loader.connect()
        db_loader.create_tables()
        print("ClickHouse Loader initialized and tables created")
    except Exception as e:
        print(f"Failed to initialize ClickHouse loader: {str(e)}")

    # Process all raw data files
    data_dir = "/opt/airflow/data/raw/news"
    if os.path.exists(data_dir):
        results = process_directory(data_dir, s3_loader, db_loader)
        print(
            f"Processed {results['success']} files successfully, {results['failure']} failed"
        )
    else:
        print(f"Data directory not found: {data_dir}")

    # Close the ClickHouse connection
    if db_loader:
        db_loader.disconnect()

    return True


# Create the tasks
news_collection_task = PythonOperator(
    task_id="collect_news_data",
    python_callable=collect_news_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id="process_raw_data",
    python_callable=process_raw_data,
    dag=dag,
)

# Define the task dependencies
[news_collection_task] >> process_data_task
