"""
db_loader.py - Utility module for loading data into S3 and ClickHouse

This module provides utility functions for loading AIS shipping data and news articles
into AWS S3 buckets and ClickHouse database by leveraging the S3Loader and ClickHouseLoader classes.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

from .s3_loader import S3Loader
from .clickhouse_loader import ClickHouseLoader
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def load_file_to_s3_and_db(
    file_path: str,
    s3_loader: Optional[S3Loader] = None,
    db_loader: Optional[ClickHouseLoader] = None,
) -> bool:
    """
    Load a file into S3 and ClickHouse database.
    """
    try:
        path = Path(file_path)
        file_name = path.name
        file_type = path.parent.name

        # Upload to S3 if loader is provided
        s3_key = None
        if s3_loader:
            try:
                s3_key = f"{file_type}/{file_name}"
                s3_loader.upload_file(file_path, s3_key)
                logger.info(f"Uploaded {file_path} to S3 key: {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload to S3: {str(e)}")
                return False

        # Load to database if loader is provided
        if db_loader:
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                if file_type.lower() == "ais":
                    # Handle AIS data
                    if isinstance(data, list):
                        db_loader.insert_vessel_positions(data)
                    else:
                        db_loader.insert_vessel_positions([data])
                    logger.info(f"Inserted AIS data from {file_path}")
                elif file_type.lower() == "news":
                    # Handle news data
                    if isinstance(data, list):
                        db_loader.insert_news_articles(data)
                    else:
                        db_loader.insert_news_articles([data])
                    logger.info(f"Inserted news data from {file_path}")
                else:
                    logger.warning(f"Unknown file type: {file_type}")
                    return False
            except Exception as e:
                logger.error(f"Failed to load to database: {str(e)}")
                return False

        return True
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return False


def process_directory(
    dir_path: str,
    s3_loader: Optional[S3Loader] = None,
    db_loader: Optional[ClickHouseLoader] = None,
) -> Dict[str, int]:
    """
    Process all files in directory and its subdirectories.
    """
    success = 0
    failure = 0

    for path in Path(dir_path).glob("**/*.json"):
        if load_file_to_s3_and_db(str(path), s3_loader, db_loader):
            success += 1
        else:
            failure += 1

    return {"success": success, "failure": failure}


if __name__ == "__main__":
    # Example usage
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Load data into S3 and/or ClickHouse")
    parser.add_argument("--s3", action="store_true", help="Enable S3 loading")
    parser.add_argument("--db", action="store_true", help="Enable database loading")
    parser.add_argument(
        "--init-db", action="store_true", help="Initialize database tables"
    )
    parser.add_argument("--dir", type=str, help="Process all files in directory")
    parser.add_argument("--file", type=str, help="Process a single file")

    args = parser.parse_args()

    # Create loaders based on arguments
    s3_loader_instance = None
    db_loader_instance = None

    if args.s3:
        try:
            s3_loader_instance = S3Loader()
        except Exception as e:
            logger.error(f"Failed to initialize S3 loader: {str(e)}")
            s3_loader_instance = None

    if args.db or args.init_db:
        try:
            db_loader_instance = ClickHouseLoader(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                database=os.getenv("CLICKHOUSE_DB", "canalytics"),
                user=os.getenv("CLICKHOUSE_USER", "canalytics_user"),
                password=os.getenv("CLICKHOUSE_PASSWORD", "canalytics_password"),
            )
            db_loader_instance.connect()
            if args.init_db:
                db_loader_instance.create_tables()
        except Exception as e:
            logger.error(f"Failed to initialize database loader: {str(e)}")
            db_loader_instance = None

    # Process files
    try:
        if args.file:
            load_file_to_s3_and_db(args.file, s3_loader_instance, db_loader_instance)
        elif args.dir:
            process_directory(args.dir, s3_loader_instance, db_loader_instance)
        elif not any([args.file, args.dir, args.init_db]):
            logger.warning(
                "No actions specified. Use --dir, --file, or --init-db with --s3 and/or --db flags."
            )
            parser.print_help()
    finally:
        # Disconnect from ClickHouse
        if db_loader_instance:
            db_loader_instance.disconnect()
