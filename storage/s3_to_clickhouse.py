"""
s3_to_clickhouse.py - Simplified utility for syncing data from S3 to ClickHouse
"""

import json
import logging
import tempfile
import os
from typing import Dict, Optional
from datetime import datetime

from .s3_loader import S3Loader
from .clickhouse_loader import ClickHouseLoader

logger = logging.getLogger(__name__)


class S3ToClickHouseSync:
    """Simple utility to sync data from S3 to ClickHouse."""

    def __init__(
        self,
        s3_loader: Optional[S3Loader] = None,
        db_loader: Optional[ClickHouseLoader] = None,
    ):
        """Initialize the sync utility."""
        self.s3_loader = s3_loader
        self.db_loader = db_loader

    def process_s3_file(self, s3_key: str) -> bool:
        """Process a single file from S3 and load to ClickHouse."""
        if not self.s3_loader or not self.db_loader:
            logger.error("S3 loader or DB loader not initialized")
            return False

        try:
            # Create a temporary file to download the S3 object
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
                temp_path = temp_file.name

            # Download the file from S3
            if not self.s3_loader.download_file(s3_key, temp_path):
                logger.error(f"Failed to download S3 file: {s3_key}")
                os.unlink(temp_path)
                return False

            # Load the JSON data
            try:
                with open(temp_path, "r") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in file: {s3_key}")
                os.unlink(temp_path)
                return False

            # Process based on file type
            success = False
            if "ais/" in s3_key:
                # Process AIS data
                if isinstance(data, list):
                    self.db_loader.insert_vessel_positions(data)
                else:
                    self.db_loader.insert_vessel_positions([data])
                success = True
            elif "news/" in s3_key:
                # Process news data
                if isinstance(data, list):
                    self.db_loader.insert_news_articles(data)
                else:
                    self.db_loader.insert_news_articles([data])
                success = True
            else:
                logger.warning(f"Unknown file type: {s3_key}")

            # Clean up the temporary file
            os.unlink(temp_path)

            if success:
                logger.info(f"Successfully processed S3 file: {s3_key}")
            return success

        except Exception as e:
            logger.error(f"Error processing S3 file {s3_key}: {str(e)}")
            return False

    def sync_from_s3(
        self,
        prefix: str = "",
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Sync data from S3 to ClickHouse.

        Args:
            prefix: S3 key prefix to filter files
            since: Only process files modified since this datetime
            limit: Maximum number of files to process

        Returns:
            Dict with success and failure counts
        """
        if not self.s3_loader:
            logger.error("S3 loader not initialized")
            return {"success": 0, "failure": 0}

        # List files from S3
        s3_files = self.s3_loader.list_files(prefix=prefix)

        # Filter by date if specified
        if since:
            s3_files = [f for f in s3_files if f["last_modified"] >= since]

        # Sort by last modified date (newest first)
        s3_files = sorted(s3_files, key=lambda x: x["last_modified"], reverse=True)

        # Apply limit if specified
        if limit and len(s3_files) > limit:
            s3_files = s3_files[:limit]

        logger.info(
            f"Found {len(s3_files)} files to process in S3 with prefix '{prefix}'"
        )

        # Process each file
        success_count = 0
        failure_count = 0

        for file_info in s3_files:
            s3_key = file_info["key"]
            if self.process_s3_file(s3_key):
                success_count += 1
            else:
                failure_count += 1

        logger.info(
            f"Sync complete: {success_count} successful, {failure_count} failed"
        )

        return {"success": success_count, "failure": failure_count}

    def process_local_file(self, file_path: str) -> bool:
        """Process a local file and load to ClickHouse."""
        if not self.db_loader:
            logger.error("DB loader not initialized")
            return False

        try:
            # Load the JSON data
            with open(file_path, "r") as f:
                data = json.load(f)

            # Process based on file type
            if "ais" in file_path.lower():
                # Process AIS data
                if isinstance(data, list):
                    self.db_loader.insert_vessel_positions(data)
                else:
                    self.db_loader.insert_vessel_positions([data])
            elif "news" in file_path.lower():
                # Process news data
                if isinstance(data, list):
                    self.db_loader.insert_news_articles(data)
                else:
                    self.db_loader.insert_news_articles([data])
            else:
                logger.warning(f"Unknown file type: {file_path}")
                return False

            logger.info(f"Successfully processed local file: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error processing local file {file_path}: {str(e)}")
            return False


def sync_all_data(
    since: Optional[datetime] = None,
    limit_per_type: Optional[int] = None,
    s3_loader: Optional[S3Loader] = None,
    db_loader: Optional[ClickHouseLoader] = None,
) -> Dict[str, Dict[str, int]]:
    """
    Convenience function to sync all types of data from S3 to ClickHouse.

    Args:
        since: Only process files modified since this datetime
        limit_per_type: Maximum number of files to process per data type
        s3_loader: S3Loader instance (optional)
        db_loader: ClickHouseLoader instance (optional)

    Returns:
        Dict with results for each data type
    """
    # Create loaders if not provided
    if not s3_loader:
        try:
            s3_loader = S3Loader()
        except Exception as e:
            logger.error(f"Failed to initialize S3 loader: {str(e)}")
            return {"error": {"success": 0, "failure": 0}}

    if not db_loader:
        try:
            # Load environment variables
            from dotenv import load_dotenv

            load_dotenv()

            # Create ClickHouse loader - will use environment variables by default
            db_loader = ClickHouseLoader()
            db_loader.connect()
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse loader: {str(e)}")
            return {"error": {"success": 0, "failure": 0}}

    # Initialize sync utility
    sync_utility = S3ToClickHouseSync(s3_loader, db_loader)

    results = {}

    # Sync AIS data
    results["ais"] = sync_utility.sync_from_s3(
        prefix="raw/ais/", since=since, limit=limit_per_type
    )

    # Sync news data
    results["news"] = sync_utility.sync_from_s3(
        prefix="raw/news/", since=since, limit=limit_per_type
    )

    return results


if __name__ == "__main__":
    # Example usage
    import argparse
    from datetime import datetime, timedelta

    parser = argparse.ArgumentParser(description="Sync data from S3 to ClickHouse")
    parser.add_argument(
        "--days", type=int, default=7, help="Process files from the last N days"
    )
    parser.add_argument("--prefix", type=str, default="", help="S3 key prefix to sync")
    parser.add_argument("--limit", type=int, help="Maximum number of files to process")
    parser.add_argument("--all", action="store_true", help="Sync all data types")
    parser.add_argument(
        "--file", action="store_true", help="Process local file(s) instead of S3"
    )
    parser.add_argument(
        "files", nargs="*", help="Local file(s) to process when using --file"
    )

    args = parser.parse_args()

    # Load environment variables
    from dotenv import load_dotenv

    load_dotenv()

    # Setup database connection - use environment variables by default
    db_loader = ClickHouseLoader()
    db_loader.connect()

    try:
        # Process local files if specified
        if args.file and args.files:
            s2c = S3ToClickHouseSync(None, db_loader)
            success_count = 0
            failure_count = 0

            for file_path in args.files:
                if s2c.process_local_file(file_path):
                    success_count += 1
                else:
                    failure_count += 1

            print(
                f"Processed {success_count} files successfully, {failure_count} failed"
            )

        # Process S3 files based on arguments
        elif args.all:
            # Calculate the start date based on the days argument
            since_date = datetime.now() - timedelta(days=args.days)

            # Sync all data types
            results = sync_all_data(
                since=since_date, limit_per_type=args.limit, db_loader=db_loader
            )

            for data_type, counts in results.items():
                print(
                    f"{data_type}: {counts['success']} successful, {counts['failure']} failed"
                )

        # Process specific prefix
        elif args.prefix:
            # Calculate the start date based on the days argument
            since_date = datetime.now() - timedelta(days=args.days)

            # Initialize S3 loader
            s3_loader = S3Loader()

            # Initialize sync utility
            s2c = S3ToClickHouseSync(s3_loader, db_loader)

            # Sync files with the specified prefix
            results = s2c.sync_from_s3(
                prefix=args.prefix, since=since_date, limit=args.limit
            )

            print(
                f"Processed {results['success']} files successfully, {results['failure']} failed"
            )

        else:
            print("No action specified. Use --all, --prefix, or --file options.")
            parser.print_help()

    finally:
        # Close the database connection
        if db_loader:
            db_loader.disconnect()
