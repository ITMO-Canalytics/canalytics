"""
s3_loader.py - Simplified S3 storage utility for Canalytics project

This module provides an S3Loader class for uploading, listing, and managing
data files in AWS S3 buckets.
"""

import os
import json
import logging
from typing import Dict, List, Union, Optional
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
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


class S3Loader:
    """Handles uploading data to AWS S3 buckets."""

    def __init__(self, bucket_name: Optional[str] = None):
        """
        Initialize S3 client and bucket.

        Args:
            bucket_name: Optional name of the S3 bucket. If not provided,
                         falls back to CANALYTICS_S3_BUCKET env variable.
        """
        self.bucket_name = bucket_name or os.getenv("CANALYTICS_S3_BUCKET")
        if not self.bucket_name:
            raise ValueError(
                "S3 bucket name not provided and CANALYTICS_S3_BUCKET not set in environment"
            )

        # Initialize S3 client with credentials from environment or instance profile
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

        logger.info(f"S3Loader initialized with bucket: {self.bucket_name}")

    def upload_file(self, local_file_path: str, s3_key: Optional[str] = None) -> bool:
        """
        Upload a file to S3 bucket.

        Args:
            local_file_path: Path to the local file to upload
            s3_key: S3 object key. If not provided, uses the filename

        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            local_path = Path(local_file_path)
            if not local_path.exists():
                logger.error(f"File not found: {local_file_path}")
                return False

            if s3_key is None:
                s3_key = local_path.name

            logger.info(
                f"Uploading {local_file_path} to s3://{self.bucket_name}/{s3_key}"
            )
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded to s3://{self.bucket_name}/{s3_key}")
            return True

        except ClientError as e:
            logger.error(f"S3 upload error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {str(e)}")
            return False

    def upload_json(self, data: Union[Dict, List], s3_key: str) -> bool:
        """
        Upload JSON data directly to S3 without creating a local file.

        Args:
            data: Dictionary or list to upload as JSON
            s3_key: S3 object key

        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            json_data = json.dumps(data)
            logger.info(f"Uploading JSON data to s3://{self.bucket_name}/{s3_key}")
            self.s3_client.put_object(
                Body=json_data,
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType="application/json",
            )
            logger.info(
                f"Successfully uploaded JSON to s3://{self.bucket_name}/{s3_key}"
            )
            return True

        except ClientError as e:
            logger.error(f"S3 JSON upload error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 JSON upload: {str(e)}")
            return False

    def list_files(self, prefix: str = "") -> List[Dict]:
        """
        List files in the S3 bucket with the given prefix.

        Args:
            prefix: S3 key prefix to filter results

        Returns:
            List of file information dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            if "Contents" not in response:
                return []

            return [
                {
                    "key": item["Key"],
                    "size": item["Size"],
                    "last_modified": item["LastModified"],
                }
                for item in response["Contents"]
            ]

        except ClientError as e:
            logger.error(f"S3 list error: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during S3 list: {str(e)}")
            return []

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 bucket.

        Args:
            s3_key: S3 object key
            local_path: Path to save the downloaded file

        Returns:
            bool: True if download was successful, False otherwise
        """
        try:
            logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {local_path}")
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Successfully downloaded to {local_path}")
            return True

        except ClientError as e:
            logger.error(f"S3 download error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 download: {str(e)}")
            return False

    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3 bucket.

        Args:
            s3_key: S3 object key

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            logger.info(f"Deleting s3://{self.bucket_name}/{s3_key}")
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Successfully deleted s3://{self.bucket_name}/{s3_key}")
            return True

        except ClientError as e:
            logger.error(f"S3 delete error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 delete: {str(e)}")
            return False
