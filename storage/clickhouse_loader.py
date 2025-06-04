from typing import Any, Dict, List, Optional
import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """ClickHouse database loader for storing and retrieving data."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize ClickHouse connection.

        Args:
            host: ClickHouse server host
            port: ClickHouse server port
            database: Database name
            user: Username
            password: Password
        """
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "username": user,
            "password": password,
        }
        self.client = None

    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        try:
            self.client = get_client(**self.connection_params)
            logger.info("Connected to ClickHouse database")
        except ClickHouseError as e:
            logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close ClickHouse connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Disconnected from ClickHouse database")

    def create_tables(self) -> None:
        """Create necessary tables if they don't exist."""
        try:
            # Create vessels table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS vessels (
                    mmsi UInt32,
                    name String,
                    type String,
                    length Float32,
                    width Float32,
                    draft Float32,
                    created_at DateTime DEFAULT now(),
                    updated_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (mmsi, created_at)
            """)

            # Create vessel_positions table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS vessel_positions (
                    mmsi UInt32,
                    timestamp DateTime,
                    latitude Float64,
                    longitude Float64,
                    speed Float32,
                    heading Float32,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (mmsi, timestamp)
            """)

            # Create canal_transits table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS canal_transits (
                    transit_id String,
                    mmsi UInt32,
                    entry_time DateTime,
                    exit_time DateTime,
                    duration Float32,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (transit_id, entry_time)
            """)

            logger.info("Created ClickHouse tables")
        except ClickHouseError as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise

    def insert_vessel(self, vessel_data: Dict[str, Any]) -> None:
        """Insert or update vessel information.

        Args:
            vessel_data: Dictionary containing vessel information
        """
        try:
            self.client.insert(
                "vessels",
                [
                    {
                        "mmsi": vessel_data["mmsi"],
                        "name": vessel_data["name"],
                        "type": vessel_data["type"],
                        "length": vessel_data["length"],
                        "width": vessel_data["width"],
                        "draft": vessel_data["draft"],
                        "updated_at": datetime.now(),
                    }
                ],
            )
            logger.info(f"Inserted/updated vessel {vessel_data['mmsi']}")
        except ClickHouseError as e:
            logger.error(f"Failed to insert vessel: {str(e)}")
            raise

    def insert_vessel_positions(self, positions: List[Dict[str, Any]]) -> None:
        """Insert vessel position records.

        Args:
            positions: List of dictionaries containing position data
        """
        try:
            self.client.insert("vessel_positions", positions)
            logger.info(f"Inserted {len(positions)} position records")
        except ClickHouseError as e:
            logger.error(f"Failed to insert positions: {str(e)}")
            raise

    def insert_canal_transit(self, transit_data: Dict[str, Any]) -> None:
        """Insert canal transit record.

        Args:
            transit_data: Dictionary containing transit information
        """
        try:
            self.client.insert("canal_transits", [transit_data])
            logger.info(f"Inserted transit record {transit_data['transit_id']}")
        except ClickHouseError as e:
            logger.error(f"Failed to insert transit: {str(e)}")
            raise

    def get_vessel_info(self, mmsi: int) -> Optional[Dict[str, Any]]:
        """Get vessel information by MMSI.

        Args:
            mmsi: Vessel MMSI number

        Returns:
            Dictionary containing vessel information or None if not found
        """
        try:
            result = self.client.query(
                "SELECT * FROM vessels WHERE mmsi = %(mmsi)s ORDER BY updated_at DESC LIMIT 1",
                parameters={"mmsi": mmsi},
            )
            return result.named_results()[0] if result.row_count > 0 else None
        except ClickHouseError as e:
            logger.error(f"Failed to get vessel info: {str(e)}")
            raise

    def get_vessel_positions(
        self, mmsi: int, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Get vessel positions for a time range.

        Args:
            mmsi: Vessel MMSI number
            start_time: Start of time range
            end_time: End of time range

        Returns:
            DataFrame containing position records
        """
        try:
            result = self.client.query(
                """
                SELECT * FROM vessel_positions 
                WHERE mmsi = %(mmsi)s 
                AND timestamp BETWEEN %(start_time)s AND %(end_time)s
                ORDER BY timestamp
                """,
                parameters={
                    "mmsi": mmsi,
                    "start_time": start_time,
                    "end_time": end_time,
                },
            )
            return result.df()
        except ClickHouseError as e:
            logger.error(f"Failed to get vessel positions: {str(e)}")
            raise

    def get_canal_transits(
        self, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Get canal transits for a time range.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            DataFrame containing transit records
        """
        try:
            result = self.client.query(
                """
                SELECT * FROM canal_transits 
                WHERE entry_time BETWEEN %(start_time)s AND %(end_time)s
                ORDER BY entry_time
                """,
                parameters={"start_time": start_time, "end_time": end_time},
            )
            return result.df()
        except ClickHouseError as e:
            logger.error(f"Failed to get canal transits: {str(e)}")
            raise
