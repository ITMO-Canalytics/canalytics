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

    # Run the collector (data goes directly to S3)
    collector = NewsCollector()
    collector.collect()
    return True


# Task to validate data collection
def validate_data_collection():
    """Validate that news data was collected and stored properly."""
    import logging
    from storage import ClickHouseLoader

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Initialize ClickHouse loader to check data
        db_loader = ClickHouseLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "canalytics"),
            user=os.getenv("CLICKHOUSE_USER", "canalytics_user"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "canalytics_password"),
        )
        db_loader.connect()

        # Check recent news data (last 24 hours)
        from datetime import datetime, timedelta

        one_day_ago = datetime.now() - timedelta(days=1)

        # Query to count recent records
        query = """
        SELECT COUNT(*) as count 
        FROM news_articles 
        WHERE created_at > %(start_date)s
        """

        result = db_loader.client.query_df(
            query, parameters={"start_date": one_day_ago.strftime("%Y-%m-%d %H:%M:%S")}
        )
        count = result["count"].iloc[0] if not result.empty else 0

        logger.info(f"Found {count} news articles collected in the last 24 hours")

        # Close the connection
        db_loader.disconnect()

        return {"articles_collected": count, "validation_successful": True}

    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return {"articles_collected": 0, "validation_successful": False}


# Create the tasks
news_collection_task = PythonOperator(
    task_id="collect_news_data",
    python_callable=collect_news_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id="validate_data_collection",
    python_callable=validate_data_collection,
    dag=dag,
)

# Define the task dependencies
[news_collection_task] >> process_data_task
