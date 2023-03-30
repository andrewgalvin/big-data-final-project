from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
import kafka
import json

consumer_key = 'va6ZBB8Ltc9hk83TQkBvmsXf4'
consumer_secret = 'D7yG6qPfXpuySEzVnQDjPin49Zri4wMgMh4LVLLF5ghTlLxTBt'
access_token = '1976888600-nlErcWZ5KY1ZldjCi2ut8uP3PTdUgefIFjaglAy'
access_secret = 'DmgvT8ZL8IZkLVPrmcczREgB0pjZWyAY2ZpqyEwKe3CLY'
kafka_brokers = 'localhost:9092'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAFVibgEAAAAAoAIQp9pM9dwmFbZNja8pRG4xvJ4%3DEJsRhgrWDTaLV96qdnma2qg0fdsKMXl4NfEdZera6EozH1JSEJ'

class TwitterStreamListener(StreamingClient):
    
    def __init__(self, token):
        super().__init__(token)
        self.producer =  KafkaProducer(bootstrap_servers=[kafka_brokers],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        api_version=(3, 4, 0))
    
    def on_tweet(self, tweet):
        print(tweet.data.keys())
        # try:
        self.producer.send('new-testing', tweet.data)
        #except kafka.errors.KafkaTimeoutError:
        #    print("Failed to update metadata")
        return True
    
    def on_error(self, status):
        print(status)
        return True

listener = TwitterStreamListener(bearer_token)
rule = StreamRule(value="a")
listener.add_rules(rule)
listener.filter()