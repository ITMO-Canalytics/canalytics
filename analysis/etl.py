"""
ETL module for Canalytics project.

This module provides functions for extracting data from PostgreSQL,
transforming it for analysis, and loading it into processed datasets.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class CanalyticsETL:
    """ETL class for transforming and processing Canalytics data."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        output_dir: str = "data/processed",
    ):
        """
        Initialize the ETL processor.

        Args:
            host: Database hostname
            port: Database port
            database: Database name
            user: Database username
            password: Database password
            output_dir: Directory to save processed data
        """
        # Database connection parameters
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "canalytics")
        self.user = user or os.getenv("POSTGRES_USER", "canalytics_user")
        self.password = password or os.getenv(
            "POSTGRES_PASSWORD", "canalytics_password"
        )

        # Output directory
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info(f"ETL initialized with output directory: {self.output_dir}")

    def get_db_connection(self):
        """Create a database connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

    def extract_ais_data(self, days: int = 7) -> pd.DataFrame:
        """
        Extract AIS data from the database.

        Args:
            days: Number of days to extract

        Returns:
            DataFrame with AIS data
        """
        try:
            conn = self.get_db_connection()

            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query data
            query = """
                SELECT 
                    id, mmsi, ship_name, latitude, longitude, 
                    cog, sog, heading, navigational_status, timestamp 
                FROM 
                    ais_data 
                WHERE 
                    timestamp >= %s 
                ORDER BY 
                    timestamp
            """

            # Load data into DataFrame
            df = pd.read_sql_query(query, conn, params=(start_date,))
            logger.info(f"Extracted {len(df)} AIS data points")

            return df

        except Exception as e:
            logger.error(f"Error extracting AIS data: {str(e)}")
            return pd.DataFrame()
        finally:
            if "conn" in locals():
                conn.close()

    def extract_news_data(self, days: int = 30) -> pd.DataFrame:
        """
        Extract news data from the database.

        Args:
            days: Number of days to extract

        Returns:
            DataFrame with news data
        """
        try:
            conn = self.get_db_connection()

            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query data
            query = """
                SELECT 
                    id, source_name, source_id, author, 
                    title, description, url, published_at 
                FROM 
                    news_articles 
                WHERE 
                    published_at >= %s 
                ORDER BY 
                    published_at DESC
            """

            # Load data into DataFrame
            df = pd.read_sql_query(query, conn, params=(start_date,))
            logger.info(f"Extracted {len(df)} news articles")

            return df

        except Exception as e:
            logger.error(f"Error extracting news data: {str(e)}")
            return pd.DataFrame()
        finally:
            if "conn" in locals():
                conn.close()

    def transform_ais_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform AIS data for analysis.

        Args:
            df: Raw AIS DataFrame

        Returns:
            Transformed DataFrame
        """
        if df.empty:
            return df

        try:
            # Make a copy to avoid modifying the original
            result = df.copy()

            # Convert timestamp to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(result["timestamp"]):
                result["timestamp"] = pd.to_datetime(result["timestamp"])

            # Calculate day and hour columns
            result["date"] = result["timestamp"].dt.date
            result["hour"] = result["timestamp"].dt.hour

            # Calculate speed in knots (already in knots in sog column)
            result["speed_knots"] = result["sog"]

            # Fill missing ship names
            result["ship_name"] = result["ship_name"].fillna("Unknown")

            # Clean up ship names (strip whitespace)
            result["ship_name"] = result["ship_name"].str.strip()

            logger.info("Transformed AIS data")
            return result

        except Exception as e:
            logger.error(f"Error transforming AIS data: {str(e)}")
            return df

    def transform_news_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform news data for analysis.

        Args:
            df: Raw news DataFrame

        Returns:
            Transformed DataFrame
        """
        if df.empty:
            return df

        try:
            # Make a copy to avoid modifying the original
            result = df.copy()

            # Convert published_at to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(result["published_at"]):
                result["published_at"] = pd.to_datetime(result["published_at"])

            # Extract publication date
            result["publication_date"] = result["published_at"].dt.date

            # Clean source names
            result["source_name"] = result["source_name"].fillna("Unknown")

            # Add a column for headline length
            result["title_length"] = result["title"].str.len()

            # Create a simple relevance score (can be enhanced later)
            # Check if titles contain keywords related to maritime shipping
            keywords = ["canal", "suez", "panama", "strait", "maritime", "shipping"]

            # Calculate a simple relevance score based on keyword presence
            def calculate_relevance(row):
                score = 0
                if isinstance(row["title"], str):
                    title_lower = row["title"].lower()
                    for keyword in keywords:
                        if keyword in title_lower:
                            score += 10

                if isinstance(row["description"], str):
                    desc_lower = row["description"].lower()
                    for keyword in keywords:
                        if keyword in desc_lower:
                            score += 5

                return score

            result["relevance_score"] = result.apply(calculate_relevance, axis=1)

            logger.info("Transformed news data")
            return result

        except Exception as e:
            logger.error(f"Error transforming news data: {str(e)}")
            return df

    def save_processed_data(self, df: pd.DataFrame, name: str) -> str:
        """
        Save processed data to CSV.

        Args:
            df: DataFrame to save
            name: Name for the file

        Returns:
            Path to the saved file
        """
        if df.empty:
            logger.warning(f"Cannot save empty DataFrame: {name}")
            return ""

        try:
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{name}_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved processed data to {filepath}")

            return filepath

        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return ""

    def run_ais_etl_pipeline(self) -> Tuple[pd.DataFrame, str]:
        """
        Run the ETL pipeline for AIS data.

        Returns:
            Tuple of (processed DataFrame, output filepath)
        """
        # Extract
        raw_df = self.extract_ais_data()

        # Transform
        processed_df = self.transform_ais_data(raw_df)

        # Load
        output_file = self.save_processed_data(processed_df, "ais_processed")

        return processed_df, output_file

    def run_news_etl_pipeline(self) -> Tuple[pd.DataFrame, str]:
        """
        Run the ETL pipeline for news data.

        Returns:
            Tuple of (processed DataFrame, output filepath)
        """
        # Extract
        raw_df = self.extract_news_data()

        # Transform
        processed_df = self.transform_news_data(raw_df)

        # Load
        output_file = self.save_processed_data(processed_df, "news_processed")

        return processed_df, output_file


if __name__ == "__main__":
    # Run ETL pipeline when script is executed directly
    etl = CanalyticsETL()

    # Run AIS pipeline
    ais_df, ais_file = etl.run_ais_etl_pipeline()
    print(f"AIS ETL complete. Processed {len(ais_df)} records. Saved to {ais_file}")

    # Run news pipeline
    news_df, news_file = etl.run_news_etl_pipeline()
    print(f"News ETL complete. Processed {len(news_df)} records. Saved to {news_file}")
