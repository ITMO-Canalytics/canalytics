import asyncio
import websockets
import json
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from storage.s3_loader import S3Loader

load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)


class AISCollectorAsync:
    def __init__(self, api_key=None, s3_loader=None):
        self.api_key = api_key or os.getenv("AIS_TOKEN")
        self.s3_loader = s3_loader or S3Loader()
        self.ws_url = "wss://stream.aisstream.io/v0/stream"
        self.message_count = 0

    async def connect_and_collect(self):
        try:
            logger.info("Connecting to AIS WebSocket...")
            async with websockets.connect(self.ws_url) as websocket:
                subscribe_message = {
                    "APIKey": self.api_key,
                    "BoundingBoxes": [
                        [[29.5, 32.0], [31.5, 33.5]],
                        [[8.8, -79.7], [9.5, -79.2]],
                        [[35.8, -6.4], [36.3, -5.5]],
                        [[41.0, 28.9], [41.3, 29.1]],
                        [[1.0, 100.0], [3.0, 104.0]],
                    ],
                    "FilterMessageTypes": ["PositionReport"],
                }

                await websocket.send(json.dumps(subscribe_message))
                logger.info("Subscribed to AIS stream, waiting for messages...")

                async for message_json in websocket:
                    message = json.loads(message_json)
                    if message.get("MessageType") == "PositionReport":
                        self.message_count += 1
                        data = message["Message"]["PositionReport"]
                        time_stamp = message["MetaData"]["time_utc"]
                        logger.info(
                            f"[{self.message_count}] timestamp: {time_stamp}, Ship: {data['UserID']} Lat: {data['Latitude']} Lon: {data['Longitude']}"
                        )
                        await self.save_data(message)

        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
            logger.info(
                f"AIS collection stopped. Total messages processed: {self.message_count}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in AIS collection: {str(e)}")

    async def save_data(self, data):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        s3_key = f"raw/ais/ais_{timestamp}.json"

        # Upload directly to S3 instead of creating local file
        success = self.s3_loader.upload_json(data, s3_key)

        if success:
            logger.info(f"Saved to S3: {s3_key}")
        else:
            logger.error(f"Failed to save to S3: {s3_key}")

    def run(self):
        asyncio.run(self.connect_and_collect())


if __name__ == "__main__":
    collector = AISCollectorAsync()
    collector.run()
