from clickhouse_driver import Client
from canalytics.config import config
from datetime import datetime, timezone

class DataLoader():
    def __init__(self):
        self.host = config.CH_HOST
        self.port = config.CH_PORT
        self.user = config.CH_USER
        self.password = config.CH_PASSWORD
        self.database = config.CH_DBNAME

    def db_load_ais(self, message):
        client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database[0]
        )

        position_report = message['Message']['PositionReport']
        metadata = message['MetaData']

        data_to_insert = [{
            # PositionReport fields
            'Cog': position_report['Cog'],
            'CommunicationState': position_report['CommunicationState'],
            'Latitude': position_report['Latitude'],
            'Longitude': position_report['Longitude'],
            'MessageID': position_report['MessageID'],
            'NavigationalStatus': position_report['NavigationalStatus'],
            'PositionAccuracy': int(position_report['PositionAccuracy']),
            'Raim': int(position_report['Raim']),
            'RateOfTurn': position_report['RateOfTurn'],
            'RepeatIndicator': position_report['RepeatIndicator'],
            'Sog': position_report['Sog'],
            'Spare': position_report['Spare'],
            'SpecialManoeuvreIndicator': position_report['SpecialManoeuvreIndicator'],
            'Timestamp': position_report['Timestamp'],
            'TrueHeading': position_report['TrueHeading'],
            'UserID': position_report['UserID'],
            'Valid': int(position_report['Valid']),

            # Other fields
            'MessageType': message['MessageType'],

            # Metadata fields
            'MMSI': metadata['MMSI'],
            'MMSI_String': metadata['MMSI_String'],
            'ShipName': metadata['ShipName'].strip(),
            'meta_latitude': metadata['latitude'],
            'meta_longitude': metadata['longitude'],
            'time_utc': datetime.strptime(metadata['time_utc'].split('.')[0], '%Y-%m-%d %H:%M:%S')
        }]

        insert_query = """
        INSERT INTO position_reports_flattened (
            Cog, CommunicationState, Latitude, Longitude,
            MessageID, NavigationalStatus, PositionAccuracy, Raim,
            RateOfTurn, RepeatIndicator, Sog, Spare,
            SpecialManoeuvreIndicator, Timestamp, TrueHeading, UserID, Valid,
            MessageType,
            MMSI, MMSI_String, ShipName, meta_latitude, meta_longitude, time_utc
        ) VALUES
        """

        client.execute(insert_query, data_to_insert)

        client.disconnect()

    def db_load_news(self, message):
        client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database[1]
        )

        data_to_insert = [{
            'source_id': article['source']['id'],
            'source_name': article['source']['name'],
            'author': article.get('author'),
            'title': article['title'],
            'description': article['description'],
            'url': article['url'],
            'urlToImage': article.get('urlToImage'),
            'publishedAt': datetime.strptime(article['publishedAt'].replace('Z', ''), '%Y-%m-%dT%H:%M:%S'),
            'content': article['content']
        } for article in message]

        # Insert query
        insert_query = """
        INSERT INTO news_articles (
            source_id, source_name, author, title,
            description, url, urlToImage, publishedAt, content
        ) VALUES
        """

        # Execute the insert
        client.execute(insert_query, data_to_insert)

        client.disconnect()
