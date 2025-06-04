import asyncio
import websockets
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from canalytics.config import config
from canalytics.storage.db_loader import DataLoader

load_dotenv()

class AISCollectorAsync:
    def __init__(self, api_key=None, output_dir='data/raw/ais'):
        self.api_key = api_key or os.getenv('AIS_TOKEN')
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.ws_url = "wss://stream.aisstream.io/v0/stream"

    async def connect_and_collect(self):
        async with websockets.connect(self.ws_url) as websocket:
            subscribe_message = {
                "APIKey": self.api_key,
                "BoundingBoxes": [
                    [[29.5, 32.0], [31.5, 33.5]],
                    [[8.8, -79.7], [9.5, -79.2]],
                    [[35.8, -6.4], [36.3, -5.5]],
                    [[41.0, 28.9], [41.3, 29.1]],
                    [[1.0, 100.0], [3.0, 104.0]]
                ],
                "FilterMessageTypes": ["PositionReport"]
            }

            await websocket.send(json.dumps(subscribe_message))

            async for message_json in websocket:
                message = json.loads(message_json)
                if message.get("MessageType") == "PositionReport":
                    data = message['Message']['PositionReport']
                    print(f"[{datetime.now(timezone.utc)}] Ship: {data['UserID']} Lat: {data['Latitude']} Lon: {data['Longitude']}")
                    await self.save_data(message)

    async def save_data(self, message):
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
        if config.STORE_LOCALLY:
            path = os.path.join(self.output_dir, f'ais_{timestamp}.json')
            with open(path, 'w') as f:
                json.dump(message, f, indent=2)
            print(f"Saved to {path}")

    def run(self):
        asyncio.run(self.connect_and_collect())

if __name__ == "__main__":
    collector = AISCollectorAsync()
    data_loader = DataLoader()
    # collector.run()
    if config.STORE_CH:
        # with open(r"..\data\raw\ais\ais_20250529_082843_558524.json") as f:
        #     message = json.load(f)
        #     data_loader.db_load_ais(message)
        with open(r"..\data\raw\news\news_2025-05-29T08-20-22.json", 'r', encoding='utf-8') as f:
            message = json.load(f)
            data_loader.db_load_news(message)
