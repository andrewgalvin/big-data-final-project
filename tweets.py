from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
import kafka
import json

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = '
kafka_brokers = 'localhost:9092'
bearer_token = ''

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
