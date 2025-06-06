"""
DAG for ETL processing of data in ClickHouse for the Canalytics project.

This DAG runs ETL ta        # Create output directory
        output_dir = "/opt/airflow/data/processed"
        os.makedirs(output_dir, exist_ok=True)

        # Initialize ETL with ClickHouse processing enabled
        etl = CanalyticsETL(output_dir=output_dir, save_to_clickhouse=True)

        # Ensure processed data tables exist
        print("Initializing processed data tables...")
        etl.initialize_processed_tables()
        print("Processed data tables ready")

        # Run AIS pipeline
        print("Running AIS ETL pipeline...")
        ais_df, ais_file = etl.run_ais_etl_pipeline()
        print(f"AIS ETL complete. Processed {len(ais_df)} records.")
        print(f"Saved to CSV: {ais_file}")
        print(f"Loaded into ClickHouse: vessel_positions_processed table")1. Extract data from ClickHouse
2. Transform it for analysis
3. Load the processed data to CSV files
4. Optionally upload the processed data back to S3
"""

from datetime import datetime, timedelta
import os
import sys
import traceback
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add parent directory to path so we can import modules
sys.path.insert(0, "/opt/airflow")

# Also add the project root directory to path just in case
project_root = Path(__file__).parent.parent.parent.parent
if project_root.exists():
    sys.path.insert(0, str(project_root))

# Default arguments for the DAG
default_args = {
    "owner": "canalytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "canalytics_etl_process",
    default_args=default_args,
    description="Run ETL process for Canalytics data",
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2025, 5, 30),
    catchup=False,
    tags=["canalytics", "etl"],
)

# Define tasks


def run_ais_etl():
    """Run ETL process for AIS data."""
    try:
        import importlib.util
        import sys
        from pathlib import Path

        # Check if we can import directly
        try:
            from analysis.etl import CanalyticsETL

            print("Successfully imported CanalyticsETL from analysis.etl")
        except ImportError:
            print("Could not import CanalyticsETL directly, trying alternative methods")

            # Try to find the analysis module
            analysis_paths = [
                "/opt/airflow/analysis",  # Standard path in Docker
                str(
                    Path(__file__).parent.parent.parent.parent / "analysis"
                ),  # Project root/analysis
            ]

            etl_module = None
            for path in analysis_paths:
                etl_path = Path(path) / "etl.py"
                print(f"Looking for etl.py at: {etl_path}")
                if etl_path.exists():
                    print(f"Found etl.py at {etl_path}")
                    # Load the module from the file path
                    spec = importlib.util.spec_from_file_location("etl", etl_path)
                    etl_module = importlib.util.module_from_spec(spec)
                    sys.modules["etl"] = etl_module
                    spec.loader.exec_module(etl_module)
                    CanalyticsETL = etl_module.CanalyticsETL
                    break

            if etl_module is None:
                raise ImportError("Could not find analysis.etl module")

        from storage import S3Loader

        # Print the available directories in /opt/airflow for debugging
        print("Contents of /opt/airflow:")
        for item in Path("/opt/airflow").glob("*"):
            print(f"  {item}")

        # Initialize ETL with output directory
        output_dir = "/opt/airflow/data/processed"
        os.makedirs(output_dir, exist_ok=True)

        etl = CanalyticsETL(output_dir=output_dir)

        # Run AIS pipeline
        print("Running AIS ETL pipeline...")
        ais_df, ais_file = etl.run_ais_etl_pipeline()
        print(f"AIS ETL complete. Processed {len(ais_df)} records. Saved to {ais_file}")

        # Optionally upload to S3
        if os.getenv("UPLOAD_ETL_RESULTS_TO_S3", "true").lower() == "true" and ais_file:
            try:
                s3_loader = S3Loader()
                s3_key = f"processed/{os.path.basename(ais_file)}"
                if s3_loader.upload_file(ais_file, s3_key):
                    print(f"Uploaded ETL results to S3: {s3_key}")
                else:
                    print("Failed to upload ETL results to S3")
            except Exception as e:
                print(f"Error uploading ETL results to S3: {str(e)}")

        return {"records_processed": len(ais_df), "output_file": ais_file}
    except Exception as e:
        print(f"Error in run_ais_etl: {str(e)}")
        traceback.print_exc()
        raise


def run_news_etl():
    """Run ETL process for news data."""
    try:
        import importlib.util
        import sys
        from pathlib import Path

        # Check if we can import directly
        try:
            from analysis.etl import CanalyticsETL

            print("Successfully imported CanalyticsETL from analysis.etl")
        except ImportError:
            print("Could not import CanalyticsETL directly, trying alternative methods")

            # Try to find the analysis module
            analysis_paths = [
                "/opt/airflow/analysis",  # Standard path in Docker
                str(
                    Path(__file__).parent.parent.parent.parent / "analysis"
                ),  # Project root/analysis
            ]

            etl_module = None
            for path in analysis_paths:
                etl_path = Path(path) / "etl.py"
                print(f"Looking for etl.py at: {etl_path}")
                if etl_path.exists():
                    print(f"Found etl.py at {etl_path}")
                    # Load the module from the file path
                    spec = importlib.util.spec_from_file_location("etl", etl_path)
                    etl_module = importlib.util.module_from_spec(spec)
                    sys.modules["etl"] = etl_module
                    spec.loader.exec_module(etl_module)
                    CanalyticsETL = etl_module.CanalyticsETL
                    break

            if etl_module is None:
                raise ImportError("Could not find analysis.etl module")

        from storage import S3Loader

        # Initialize ETL with output directory and ClickHouse processing
        output_dir = "/opt/airflow/data/processed"
        os.makedirs(output_dir, exist_ok=True)

        # Initialize ETL with ClickHouse processing enabled
        etl = CanalyticsETL(output_dir=output_dir, save_to_clickhouse=True)

        # Ensure processed data tables exist
        print("Initializing processed data tables...")
        etl.initialize_processed_tables()
        print("Processed data tables ready")

        # Run news pipeline
        print("Running news ETL pipeline...")
        news_df, news_file = etl.run_news_etl_pipeline()
        print(f"News ETL complete. Processed {len(news_df)} records.")
        print(f"Saved to CSV: {news_file}")
        print(f"Loaded into ClickHouse: news_articles_processed table")

        # Optionally upload to S3
        if (
            os.getenv("UPLOAD_ETL_RESULTS_TO_S3", "true").lower() == "true"
            and news_file
        ):
            try:
                s3_loader = S3Loader()
                s3_key = f"processed/{os.path.basename(news_file)}"
                if s3_loader.upload_file(news_file, s3_key):
                    print(f"Uploaded ETL results to S3: {s3_key}")
                else:
                    print("Failed to upload ETL results to S3")
            except Exception as e:
                print(f"Error uploading ETL results to S3: {str(e)}")

        return {"records_processed": len(news_df), "output_file": news_file}
    except Exception as e:
        print(f"Error in run_news_etl: {str(e)}")
        traceback.print_exc()
        raise


def summarize_etl_results(**context):
    """Summarize ETL process results."""
    try:
        import json
        from datetime import datetime
        import os

        # Get results from upstream tasks
        ais_results = context["ti"].xcom_pull(task_ids="run_ais_etl") or {
            "records_processed": 0
        }
        news_results = context["ti"].xcom_pull(task_ids="run_news_etl") or {
            "records_processed": 0
        }

        # Create summary
        summary = {
            "timestamp": datetime.now().isoformat(),
            "ais_etl": ais_results,
            "news_etl": news_results,
            "total_records_processed": ais_results.get("records_processed", 0)
            + news_results.get("records_processed", 0),
        }

        # Save summary to file
        os.makedirs("/opt/airflow/data/reports", exist_ok=True)
        report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"/opt/airflow/data/reports/etl_report_{report_time}.json"

        with open(report_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"ETL summary report generated: {report_path}")
        print(f"Total records processed: {summary['total_records_processed']}")

        return summary
    except Exception as e:
        print(f"Error in summarize_etl_results: {str(e)}")
        traceback.print_exc()
        raise


# Create the tasks
ais_etl_task = PythonOperator(
    task_id="run_ais_etl",
    python_callable=run_ais_etl,
    dag=dag,
)

news_etl_task = PythonOperator(
    task_id="run_news_etl",
    python_callable=run_news_etl,
    dag=dag,
)

summarize_task = PythonOperator(
    task_id="summarize_etl_results",
    python_callable=summarize_etl_results,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
[ais_etl_task, news_etl_task] >> summarize_task
