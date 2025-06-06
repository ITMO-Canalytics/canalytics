"""
ETL module for Canalytics project.

This module provides functions for extracting data from ClickHouse,
transforming it for analysis, and loading it into processed datasets.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional, Tuple
from clickhouse_connect import get_client
import sys

# Add the parent directory to the path to import storage modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.clickhouse_loader import ClickHouseLoader

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
        save_to_clickhouse: bool = True,
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
            save_to_clickhouse: Whether to save processed data to ClickHouse
        """
        # ClickHouse connection parameters
        self.host = host or os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.database = database or os.getenv("CLICKHOUSE_DB", "default")
        self.user = user or os.getenv("CLICKHOUSE_USER", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "")

        # Output directory
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # ClickHouse saving option
        self.save_to_clickhouse = save_to_clickhouse

        # Initialize ClickHouse loader if needed
        self.clickhouse_loader = None
        if self.save_to_clickhouse:
            self.clickhouse_loader = ClickHouseLoader(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )

        logger.info(f"ETL initialized with output directory: {self.output_dir}")
        if self.save_to_clickhouse:
            logger.info("ClickHouse loading enabled for processed data")

    def get_db_client(self):
        """Create a ClickHouse client connection."""
        return get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
        )

    def extract_ais_data(self, days: int = 7) -> pd.DataFrame:
        """
        Extract AIS data from ClickHouse.

        Args:
            days: Number of days to extract

        Returns:
            DataFrame with AIS data
        """
        try:
            client = self.get_db_client()

            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query data from vessel_positions table
            query = """
                SELECT 
                    mmsi, timestamp, latitude, longitude, 
                    speed, heading, created_at
                FROM 
                    vessel_positions 
                WHERE 
                    timestamp >= %(start_date)s 
                ORDER BY 
                    timestamp
            """

            # Execute query
            result = client.query_df(query, {"start_date": start_date})
            logger.info(f"Extracted {len(result)} AIS data points")

            return result

        except Exception as e:
            logger.error(f"Error extracting AIS data: {str(e)}")
            return pd.DataFrame()
        finally:
            if "client" in locals():
                client.close()

    def extract_news_data(self, days: int = 30) -> pd.DataFrame:
        """
        Extract news data from ClickHouse.

        Args:
            days: Number of days to extract

        Returns:
            DataFrame with news data
        """
        try:
            client = self.get_db_client()

            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query data from news_articles table
            query = """
                SELECT 
                    article_id, title, description, content, author,
                    source_name, source_id, url, url_to_image, 
                    published_at, created_at
                FROM 
                    news_articles 
                WHERE 
                    published_at >= %(start_date)s 
                ORDER BY 
                    published_at DESC
            """

            # Execute query
            result = client.query_df(query, {"start_date": start_date})
            logger.info(f"Extracted {len(result)} news articles")

            return result

        except Exception as e:
            logger.error(f"Error extracting news data: {str(e)}")
            return pd.DataFrame()
        finally:
            if "client" in locals():
                client.close()

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

            # Speed is already in correct units from ClickHouse
            result["speed_knots"] = result["speed"]

            # Add vessel information (you could join with vessels table if needed)
            result["mmsi_str"] = result["mmsi"].astype(str)

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
            keywords = [
                "canal",
                "suez",
                "panama",
                "strait",
                "maritime",
                "shipping",
                "vessel",
                "port",
                "cargo",
            ]

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
        Save processed data to CSV and optionally to ClickHouse.

        Args:
            df: DataFrame to save
            name: Name for the file and data type

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
            logger.info(f"Saved processed data to CSV: {filepath}")

            # Save to ClickHouse if enabled
            if self.save_to_clickhouse and self.clickhouse_loader:
                try:
                    self.clickhouse_loader.connect()

                    if "ais" in name.lower():
                        self.clickhouse_loader.insert_processed_vessel_positions(df)
                        logger.info(
                            f"Saved {len(df)} processed AIS records to ClickHouse"
                        )
                    elif "news" in name.lower():
                        self.clickhouse_loader.insert_processed_news_articles(df)
                        logger.info(
                            f"Saved {len(df)} processed news records to ClickHouse"
                        )
                    else:
                        logger.warning(
                            f"Unknown data type for ClickHouse saving: {name}"
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to save processed data to ClickHouse: {str(e)}"
                    )
                    # Don't raise exception - CSV saving succeeded
                finally:
                    if self.clickhouse_loader:
                        self.clickhouse_loader.disconnect()

            return filepath

        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return ""

    def initialize_processed_tables(self) -> None:
        """Initialize processed data tables in ClickHouse."""
        if not self.clickhouse_loader:
            logger.warning("ClickHouse loader not initialized")
            return

        try:
            self.clickhouse_loader.connect()
            self.clickhouse_loader.create_tables()
            logger.info("Initialized processed data tables in ClickHouse")
        except Exception as e:
            logger.error(f"Failed to initialize processed tables: {str(e)}")
            raise
        finally:
            self.clickhouse_loader.disconnect()

    def get_processed_data_for_analysis(
        self, data_type: str = "ais", **kwargs
    ) -> pd.DataFrame:
        """
        Get processed data from ClickHouse for analysis.

        Args:
            data_type: Type of data to retrieve ("ais" or "news")
            **kwargs: Additional filters (start_date, end_date, mmsi, min_relevance, etc.)

        Returns:
            DataFrame containing processed data
        """
        if not self.clickhouse_loader:
            logger.error("ClickHouse loader not initialized")
            return pd.DataFrame()

        try:
            self.clickhouse_loader.connect()

            if data_type.lower() == "ais":
                return self.clickhouse_loader.get_processed_vessel_positions(**kwargs)
            elif data_type.lower() == "news":
                return self.clickhouse_loader.get_processed_news_articles(**kwargs)
            else:
                logger.error(f"Unknown data type: {data_type}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to get processed data: {str(e)}")
            return pd.DataFrame()
        finally:
            self.clickhouse_loader.disconnect()

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
