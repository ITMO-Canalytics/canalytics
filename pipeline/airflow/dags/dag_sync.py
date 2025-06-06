"""
DAG for syncing data from S3 to ClickHouse for the Canalytics project.

This DAG runs a task that:
1. Syncs AIS data from S3 to ClickHouse
2. Syncs news data from S3 to ClickHouse
3. Provides a status report of the sync process
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
    "canalytics_sync_s3_to_clickhouse",
    default_args=default_args,
    description="Sync data from S3 to ClickHouse for Canalytics",
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2025, 5, 30),
    catchup=False,
    tags=["canalytics", "data_sync"],
)


def sync_all_data():
    """Sync both AIS and news data from S3 to ClickHouse."""
    import logging
    from storage import S3Loader, ClickHouseLoader
    from storage.s3_to_clickhouse import sync_all_data
    from datetime import datetime, timedelta
    import json

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    s3_loader = None
    db_loader = None

    try:
        # Create S3 loader
        try:
            s3_loader = S3Loader()
            logger.info("S3 loader initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize S3 loader: {str(e)}")
            raise

        # Create ClickHouse loader
        try:
            db_loader = ClickHouseLoader(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                database=os.getenv("CLICKHOUSE_DB", "canalytics"),
                user=os.getenv("CLICKHOUSE_USER", "canalytics_user"),
                password=os.getenv("CLICKHOUSE_PASSWORD", "canalytics_password"),
            )
            db_loader.connect()
            logger.info("ClickHouse loader initialized successfully")

            # Ensure tables exist
            db_loader.create_tables()
            logger.info("ClickHouse tables created/verified")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse loader: {str(e)}")
            raise

        # Sync data from the last 24 hours (or as per ENV variable)
        days_to_sync = int(os.getenv("SYNC_LOOKBACK_DAYS", "1"))
        from datetime import timezone

        since_date = datetime.now(timezone.utc) - timedelta(days=days_to_sync)
        logger.info(f"Syncing data since {since_date}")

        # Sync all data types
        results = sync_all_data(
            since=since_date, s3_loader=s3_loader, db_loader=db_loader
        )

        # Create report file
        os.makedirs("/opt/airflow/data/reports", exist_ok=True)
        report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"/opt/airflow/data/reports/sync_report_{report_time}.json"

        # Prepare report
        success_count = sum(
            type_result.get("success", 0) for type_result in results.values()
        )
        failure_count = sum(
            type_result.get("failure", 0) for type_result in results.values()
        )
        total_count = success_count + failure_count

        report = {
            "timestamp": datetime.now().isoformat(),
            "sync_period": f"Last {days_to_sync} days",
            "since_date": since_date.isoformat(),
            "summary": {
                "success_count": success_count,
                "failure_count": failure_count,
                "total_count": total_count,
                "success_rate": round((success_count / total_count) * 100, 2)
                if total_count > 0
                else 100,
            },
            "details": results,
        }

        # Save report
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Sync report generated: {report_path}")
        logger.info(
            f"Summary: {success_count} succeeded, {failure_count} failed out of {total_count} total"
        )

        # Log detailed results
        for data_type, counts in results.items():
            logger.info(
                f"{data_type}: {counts.get('success', 0)} successful, {counts.get('failure', 0)} failed"
            )

        return report["summary"]

    except Exception as e:
        logger.error(f"Error in sync process: {str(e)}")
        # Try to create an error report
        try:
            os.makedirs("/opt/airflow/data/reports", exist_ok=True)
            report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_report_path = (
                f"/opt/airflow/data/reports/sync_error_{report_time}.json"
            )

            error_report = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "status": "failed",
            }

            with open(error_report_path, "w") as f:
                json.dump(error_report, f, indent=2, default=str)

            logger.info(f"Error report generated: {error_report_path}")
        except Exception as report_error:
            logger.error(f"Failed to create error report: {str(report_error)}")

        raise
    finally:
        # Ensure proper cleanup
        if db_loader:
            try:
                db_loader.disconnect()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.error(f"Error closing ClickHouse connection: {str(e)}")

        if s3_loader:
            logger.info("S3 loader cleanup complete")


# Create the task
sync_task = PythonOperator(
    task_id="sync_all_data",
    python_callable=sync_all_data,
    dag=dag,
)
