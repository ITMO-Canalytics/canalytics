import os
import requests
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from storage.s3_loader import S3Loader

load_dotenv()


class NewsCollector:
    def __init__(self, s3_loader=None):
        self.api_key = os.getenv("NEWS_API_KEY")
        if not self.api_key:
            raise ValueError("NEWS_API_KEY not found in environment variables")

        self.s3_loader = s3_loader or S3Loader()

    def collect(self):
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "Suez Canal OR Panama Canal OR Strait of Gibraltar OR Bosporus Strait OR Strait of Malacca",
            "language": "en",
            "pageSize": 30,
            "apiKey": self.api_key,
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error fetching news: {response.status_code} - {response.text}")
            return

        data = response.json()
        print(f"Response status: {response.status_code}")
        print(f"Total results: {data.get('totalResults', 0)}")

        articles = data.get("articles", [])
        if not articles:
            print("No articles found.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        s3_key = f"raw/news/news_{ts}.json"

        # Upload directly to S3 instead of creating local file
        success = self.s3_loader.upload_json(articles, s3_key)

        if success:
            print(f"Saved {len(articles)} news articles to S3: {s3_key}")
        else:
            print(f"Failed to save articles to S3: {s3_key}")


if __name__ == "__main__":
    collector = NewsCollector()
    collector.collect()
