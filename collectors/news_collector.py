import os
import requests
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

class NewsCollector:
    def __init__(self):
        self.api_key = os.getenv("NEWS_API_KEY")
        if not self.api_key:
            raise ValueError("NEWS_API_KEY not found in environment variables")

    def collect(self):
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "Suez Canal OR Panama Canal OR Strait of Gibraltar OR Bosporus Strait OR Strait of Malacca",
            "language": "en",
            "pageSize": 30,
            "apiKey": self.api_key
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
        output_dir = "data/raw/news"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"news_{ts}.json")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(articles)} news articles to {output_file}")

if __name__ == "__main__":
    collector = NewsCollector()
    collector.collect()
