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
            self.client.command(
                """
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
            """
            )

            # Create vessel_positions table
            self.client.command(
                """
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
            """
            )

            # Create canal_transits table
            self.client.command(
                """
                CREATE TABLE IF NOT EXISTS canal_transits (
                    transit_id String,
                    mmsi UInt32,
                    entry_time DateTime,
                    exit_time DateTime,
                    duration Float32,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (transit_id, entry_time)
            """
            )

            # Create news_articles table
            self.client.command(
                """
                CREATE TABLE IF NOT EXISTS news_articles (
                    article_id String,
                    title String,
                    description String,
                    content String,
                    author String,
                    source_name String,
                    source_id Nullable(String),
                    url String,
                    url_to_image Nullable(String),
                    published_at DateTime,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (published_at, article_id)
            """
            )

            # Create vessel_positions_processed table for ETL-transformed AIS data
            self.client.command(
                """
                CREATE TABLE IF NOT EXISTS vessel_positions_processed (
                    mmsi UInt32,
                    timestamp DateTime,
                    latitude Float64,
                    longitude Float64,
                    speed Float32,
                    heading Float32,
                    date Date,
                    hour UInt8,
                    speed_knots Float32,
                    mmsi_str String,
                    created_at DateTime DEFAULT now(),
                    processed_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (date, mmsi, timestamp)
            """
            )

            # Create news_articles_processed table for ETL-transformed news data
            self.client.command(
                """
                CREATE TABLE IF NOT EXISTS news_articles_processed (
                    article_id String,
                    title String,
                    description String,
                    content String,
                    author String,
                    source_name String,
                    source_id Nullable(String),
                    url String,
                    url_to_image Nullable(String),
                    published_at DateTime,
                    publication_date Date,
                    title_length UInt16,
                    relevance_score UInt16,
                    created_at DateTime DEFAULT now(),
                    processed_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (publication_date, article_id)
            """
            )

            logger.info("Created ClickHouse tables including processed data tables")
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
            # Transform AIS data to match vessel_positions table schema
            transformed_positions = []
            for pos in positions:
                # Handle different data structures from AIS files
                if "MetaData" in pos and "Message" in pos:
                    # Standard AIS format
                    metadata = pos["MetaData"]
                    message = pos["Message"].get("PositionReport", {})

                    # Parse time_utc field properly
                    time_str = metadata.get("time_utc", "")
                    if time_str:
                        # Handle format like "2025-05-29 08:28:43.529530119 +0000 UTC"
                        try:
                            # Remove timezone info and microseconds for ClickHouse DateTime
                            time_clean = time_str.split(".")[0].replace(
                                " +0000 UTC", ""
                            )
                            timestamp = datetime.strptime(
                                time_clean, "%Y-%m-%d %H:%M:%S"
                            )
                        except ValueError:
                            logger.warning(
                                f"Could not parse timestamp: {time_str}, using current time"
                            )
                            timestamp = datetime.now()
                    else:
                        timestamp = datetime.now()

                    transformed_positions.append(
                        {
                            "mmsi": metadata.get("MMSI", 0),
                            "timestamp": timestamp,
                            "latitude": metadata.get("latitude", 0.0),
                            "longitude": metadata.get("longitude", 0.0),
                            "speed": message.get("Sog", 0.0),  # Speed over ground
                            "heading": message.get("Cog", 0.0),  # Course over ground
                            "created_at": datetime.now(),
                        }
                    )
                else:
                    # Direct format - assume it matches table schema
                    if "timestamp" in pos and isinstance(pos["timestamp"], str):
                        try:
                            pos["timestamp"] = datetime.fromisoformat(
                                pos["timestamp"].replace("Z", "+00:00")
                            )
                        except ValueError:
                            pos["timestamp"] = datetime.now()

                    # Ensure created_at is present
                    if "created_at" not in pos:
                        pos["created_at"] = datetime.now()

                    transformed_positions.append(pos)

            if transformed_positions:
                # Debug: Log the data being inserted
                logger.info(
                    f"About to insert {len(transformed_positions)} position records"
                )
                for i, record in enumerate(
                    transformed_positions[:2]
                ):  # Log first 2 records
                    logger.info(f"Record {i}: {record}")
                    for key, value in record.items():
                        logger.info(f"  {key}: {value} (type: {type(value)})")

                # Convert dictionaries to column-ordered data for ClickHouse
                # Table schema: mmsi, timestamp, latitude, longitude, speed, heading, created_at
                column_data = []
                for record in transformed_positions:
                    column_data.append(
                        [
                            record["mmsi"],
                            record["timestamp"],
                            record["latitude"],
                            record["longitude"],
                            record["speed"],
                            record["heading"],
                            record["created_at"],
                        ]
                    )

                logger.info(
                    f"Converted to column data format: {len(column_data)} records"
                )
                if column_data:
                    logger.info(f"Sample column record: {column_data[0]}")

                # Insert with explicit column specification
                self.client.insert(
                    "vessel_positions",
                    column_data,
                    column_names=[
                        "mmsi",
                        "timestamp",
                        "latitude",
                        "longitude",
                        "speed",
                        "heading",
                        "created_at",
                    ],
                )
                logger.info(f"Inserted {len(transformed_positions)} position records")
            else:
                logger.warning("No valid position records to insert")
        except ClickHouseError as e:
            logger.error(f"Failed to insert positions: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error transforming position data: {str(e)}")
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

    def insert_news_articles(self, articles: List[Dict[str, Any]]) -> None:
        """Insert news article records.

        Args:
            articles: List of dictionaries containing news article data
        """
        try:
            # Handle different news data structures
            transformed_articles = []

            logger.info(f"Processing {len(articles)} news article entries")

            for article_data in articles:
                # Check if this is a valid article object
                if article_data is None:
                    logger.warning("Skipping None article")
                    continue

                if not isinstance(article_data, dict):
                    logger.warning(f"Skipping non-dict article: {type(article_data)}")
                    continue

                # Handle NewsAPI response format
                if "articles" in article_data:
                    # This is a NewsAPI response wrapper
                    logger.info("Processing NewsAPI response wrapper format")
                    for article in article_data["articles"]:
                        if article is not None:
                            transformed_article = self._transform_news_article(article)
                            if transformed_article:
                                transformed_articles.append(transformed_article)
                elif "title" in article_data or "url" in article_data:
                    # This is a direct article object
                    logger.debug(
                        f"Processing direct article: {article_data.get('title', 'No title')[:50]}..."
                    )
                    transformed_article = self._transform_news_article(article_data)
                    if transformed_article:
                        transformed_articles.append(transformed_article)
                else:
                    logger.warning(
                        f"Unknown news article format, keys: {list(article_data.keys()) if isinstance(article_data, dict) else type(article_data)}"
                    )

            if transformed_articles:
                # Convert dictionaries to column-ordered data for ClickHouse, similar to AIS data
                # Table schema: article_id, title, description, content, author, source_name, source_id, url, url_to_image, published_at, created_at
                column_data = []
                for record in transformed_articles:
                    column_data.append(
                        [
                            record["article_id"],
                            record["title"],
                            record["description"],
                            record["content"],
                            record["author"],
                            record["source_name"],
                            record["source_id"],
                            record["url"],
                            record["url_to_image"],
                            record["published_at"],
                            record["created_at"],
                        ]
                    )

                logger.info(
                    f"Converted to column data format: {len(column_data)} records"
                )
                if column_data:
                    logger.info(
                        f"Sample column record keys: article_id='{column_data[0][0][:50]}...', title='{column_data[0][1][:50]}...'"
                    )

                # Insert with explicit column specification
                self.client.insert(
                    "news_articles",
                    column_data,
                    column_names=[
                        "article_id",
                        "title",
                        "description",
                        "content",
                        "author",
                        "source_name",
                        "source_id",
                        "url",
                        "url_to_image",
                        "published_at",
                        "created_at",
                    ],
                )
                logger.info(f"Inserted {len(transformed_articles)} news articles")
            else:
                logger.warning("No valid news articles to insert")
        except ClickHouseError as e:
            logger.error(f"Failed to insert news articles: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error transforming news data: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(
                f"Articles data sample: {articles[:1] if articles else 'Empty list'}"
            )
            raise

    def _transform_news_article(
        self, article: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transform a single news article to match the database schema.

        Args:
            article: Dictionary containing news article data

        Returns:
            Transformed article dictionary or None if invalid
        """
        try:
            # Validate that we have essential data
            if not article or not isinstance(article, dict):
                logger.warning("Invalid article: not a dictionary")
                return None

            # Skip articles without title or URL (essential fields)
            if not article.get("title") and not article.get("url"):
                logger.warning("Skipping article without title or URL")
                return None

            # Parse published date
            published_at_str = article.get("publishedAt", "")
            if published_at_str:
                try:
                    # Handle ISO format with Z timezone
                    published_at = datetime.fromisoformat(
                        published_at_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    logger.warning(
                        f"Could not parse publishedAt: {published_at_str}, using current time"
                    )
                    published_at = datetime.now()
            else:
                published_at = datetime.now()

            # Generate article ID from URL or title
            article_id = article.get("url", "")
            if not article_id:
                article_id = f"article_{hash(article.get('title', ''))}"

            # Extract source information safely
            source = article.get("source", {})
            if source is None:
                source = {}

            source_name = ""
            source_id = None

            if isinstance(source, dict):
                source_name = source.get("name", "") or ""
                source_id = source.get("id")
            else:
                source_name = str(source) if source else ""

            # Safe string extraction with None handling
            def safe_str(value, max_length=None):
                if value is None:
                    return ""
                str_value = str(value)
                if max_length:
                    return str_value[:max_length]
                return str_value

            return {
                "article_id": safe_str(article_id, 500),
                "title": safe_str(article.get("title"), 500),
                "description": safe_str(article.get("description"), 1000),
                "content": safe_str(article.get("content"), 5000),
                "author": safe_str(article.get("author"), 200),
                "source_name": safe_str(source_name, 100),
                "source_id": source_id,
                "url": safe_str(article.get("url"), 500),
                "url_to_image": safe_str(article.get("urlToImage"), 500),
                "published_at": published_at,
                "created_at": datetime.now(),
            }
        except Exception as e:
            logger.error(f"Error transforming article: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(
                f"Article keys: {list(article.keys()) if isinstance(article, dict) else 'Not a dict'}"
            )
            if isinstance(article, dict):
                logger.error(
                    f"Article sample: title='{article.get('title', 'N/A')}', url='{article.get('url', 'N/A')}'"
                )
            return None

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

    def insert_processed_vessel_positions(self, positions_df: pd.DataFrame) -> None:
        """Insert processed vessel position records from ETL pipeline.

        Args:
            positions_df: DataFrame containing processed AIS data with additional analytical columns
        """
        if positions_df.empty:
            logger.warning("No processed vessel positions to insert")
            return

        try:
            # Convert DataFrame to list of records for ClickHouse
            records = []
            for _, row in positions_df.iterrows():
                record = [
                    int(row["mmsi"]),
                    row["timestamp"],
                    float(row["latitude"]),
                    float(row["longitude"]),
                    float(row["speed"]),
                    float(row["heading"]),
                    row["date"],
                    int(row["hour"]),
                    float(row["speed_knots"]),
                    str(row["mmsi_str"]),
                    row.get("created_at", datetime.now()),
                    datetime.now(),  # processed_at
                ]
                records.append(record)

            # Insert with explicit column specification
            self.client.insert(
                "vessel_positions_processed",
                records,
                column_names=[
                    "mmsi",
                    "timestamp",
                    "latitude",
                    "longitude",
                    "speed",
                    "heading",
                    "date",
                    "hour",
                    "speed_knots",
                    "mmsi_str",
                    "created_at",
                    "processed_at",
                ],
            )
            logger.info(f"Inserted {len(records)} processed vessel position records")
        except Exception as e:
            logger.error(f"Failed to insert processed vessel positions: {str(e)}")
            raise

    def insert_processed_news_articles(self, news_df: pd.DataFrame) -> None:
        """Insert processed news article records from ETL pipeline.

        Args:
            news_df: DataFrame containing processed news data with additional analytical columns
        """
        if news_df.empty:
            logger.warning("No processed news articles to insert")
            return

        try:
            # Convert DataFrame to list of records for ClickHouse
            records = []
            for _, row in news_df.iterrows():
                record = [
                    str(row["article_id"]),
                    str(row["title"]),
                    str(row["description"]) if pd.notna(row["description"]) else "",
                    str(row["content"]) if pd.notna(row["content"]) else "",
                    str(row["author"]) if pd.notna(row["author"]) else "",
                    str(row["source_name"]),
                    str(row["source_id"]) if pd.notna(row["source_id"]) else None,
                    str(row["url"]),
                    str(row["url_to_image"]) if pd.notna(row["url_to_image"]) else None,
                    row["published_at"],
                    row["publication_date"],
                    int(row["title_length"]),
                    int(row["relevance_score"]),
                    row.get("created_at", datetime.now()),
                    datetime.now(),  # processed_at
                ]
                records.append(record)

            # Insert with explicit column specification
            self.client.insert(
                "news_articles_processed",
                records,
                column_names=[
                    "article_id",
                    "title",
                    "description",
                    "content",
                    "author",
                    "source_name",
                    "source_id",
                    "url",
                    "url_to_image",
                    "published_at",
                    "publication_date",
                    "title_length",
                    "relevance_score",
                    "created_at",
                    "processed_at",
                ],
            )
            logger.info(f"Inserted {len(records)} processed news article records")
        except Exception as e:
            logger.error(f"Failed to insert processed news articles: {str(e)}")
            raise

    def get_processed_vessel_positions(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mmsi: Optional[int] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Get processed vessel positions for analysis.

        Args:
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            mmsi: Filter by specific vessel MMSI
            limit: Maximum number of records to return

        Returns:
            DataFrame containing processed vessel position records
        """
        try:
            where_conditions = []
            parameters = {}

            if start_date:
                where_conditions.append("date >= %(start_date)s")
                parameters["start_date"] = start_date

            if end_date:
                where_conditions.append("date <= %(end_date)s")
                parameters["end_date"] = end_date

            if mmsi:
                where_conditions.append("mmsi = %(mmsi)s")
                parameters["mmsi"] = mmsi

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT * FROM vessel_positions_processed 
                {where_clause}
                ORDER BY date DESC, timestamp DESC
                LIMIT {limit}
            """

            result = self.client.query_df(query, parameters=parameters)
            return result
        except ClickHouseError as e:
            logger.error(f"Failed to get processed vessel positions: {str(e)}")
            raise

    def get_processed_news_articles(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        min_relevance: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get processed news articles for analysis.

        Args:
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            min_relevance: Minimum relevance score filter
            limit: Maximum number of records to return

        Returns:
            DataFrame containing processed news article records
        """
        try:
            where_conditions = []
            parameters = {}

            if start_date:
                where_conditions.append("publication_date >= %(start_date)s")
                parameters["start_date"] = start_date

            if end_date:
                where_conditions.append("publication_date <= %(end_date)s")
                parameters["end_date"] = end_date

            if min_relevance > 0:
                where_conditions.append("relevance_score >= %(min_relevance)s")
                parameters["min_relevance"] = min_relevance

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT * FROM news_articles_processed 
                {where_clause}
                ORDER BY publication_date DESC, relevance_score DESC
                LIMIT {limit}
            """

            result = self.client.query_df(query, parameters=parameters)
            return result
        except ClickHouseError as e:
            logger.error(f"Failed to get processed news articles: {str(e)}")
            raise
