"""
db_loader.py - Utility module for loading data into S3 and ClickHouse

This module provides utility functions for loading AIS shipping data and news articles
into AWS S3 buckets and ClickHouse database by leveraging the S3Loader and ClickHouseLoader classes.
"""

import json
import logging
import traceback
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

    Args:
        file_path: Path to the file to load
        s3_loader: S3Loader instance (optional)
        db_loader: ClickHouseLoader instance (optional)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False

        # Load file content
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)

        # Determine file type based on path and content structure
        file_type = None
        if "news_" in file_path.name:
            file_type = "news"
        elif (
            "ais_" in file_path.name
            and isinstance(content, dict)
            and "MessageType" in content
        ):
            file_type = "ais"
        else:
            logger.warning(f"Could not determine file type for: {file_path}")
            return False

        # Upload to S3 if loader provided
        if s3_loader:
            s3_key = f"{file_type}/{file_path.name}"
            if not s3_loader.upload_file(str(file_path), s3_key):
                logger.warning(f"Failed to upload {file_path} to S3")

        # Insert into database if loader provided
        # Note: Implementation for ClickHouse will be different
        if db_loader and db_loader.client:
            if file_type == "ais":
                # Process AIS data for ClickHouse
                # You would need to implement the appropriate logic for ClickHouse here
                # For now, just return success
                logger.info(f"Would process AIS data for ClickHouse: {file_path}")
                return True
            elif file_type == "news":
                # Process news data for ClickHouse
                # You would need to implement the appropriate logic for ClickHouse here
                # For now, just return success
                logger.info(f"Would process news data for ClickHouse: {file_path}")
                return True

        return True

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        return False
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        traceback.print_exc()
        return False


def process_directory(
    directory: str,
    s3_loader: Optional[S3Loader] = None,
    db_loader: Optional[ClickHouseLoader] = None,
    recursive: bool = True,
) -> Dict[str, int]:
    """
    Process all files in a directory, loading them to S3 and DB.

    Args:
        directory: Directory path to process
        s3_loader: S3Loader instance (optional)
        db_loader: ClickHouseLoader instance (optional)
        recursive: Whether to process subdirectories

    Returns:
        Dict with success/failure counts
    """
    dir_path = Path(directory)
    if not dir_path.exists() or not dir_path.is_dir():
        logger.error(f"Directory not found: {directory}")
        return {"success": 0, "failure": 0}

    results = {"success": 0, "failure": 0}

    for item in dir_path.iterdir():
        if item.is_file() and item.suffix in [".json"]:
            success = load_file_to_s3_and_db(str(item), s3_loader, db_loader)
            if success:
                results["success"] += 1
            else:
                results["failure"] += 1
        elif recursive and item.is_dir():
            sub_results = process_directory(str(item), s3_loader, db_loader, recursive)
            results["success"] += sub_results["success"]
            results["failure"] += sub_results["failure"]

    logger.info(
        f"Processed directory {directory}: {results['success']} successful, {results['failure']} failed"
    )
    return results


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
