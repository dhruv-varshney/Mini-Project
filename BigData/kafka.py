import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
consumer_key = "b5fGAXITNPyLWoZ9glyDhebuK"
consumer_secret = "AITERoZOQGwq5ZUbkWsl6iF1vqD18vNp2dixSttFzFhMmxV5mb"
access_token = "1355447968292192256-PIX8dBwtAp4dMGgTZxbHCSzUFVXk4S"
access_token_secret = "ar4iAu2dncskUPTrlaXnBSTzKHNhf38dTNzbKmacuYIJH"
auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)
from datetime import datetime
def normalize_timestamp(time):
  mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
  return(mytime.strftime("%Y-%m-%d %H:%M:%S"))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(2,0,2))
topic_name = 'dhruv2612'
def get_twitter_data():
  res = api.search("BCCI")
  for i in res:
    record = ''
    record += str(i.user.id_str)
    record = '\n'
    record += str(i.text)
    record += ';\n'
    record += str(normalize_timestamp(str(i.created_at)))
    record += ';'
    record += str(i.user.followers_count)
    record += ';'
    record += str(i.user.location)
    record += ';'
    record += str(i.favorite_count)
    record += ';'
    record += str(i.retweet_count)
    record += ';'
    producer.send(topic_name, str.encode(record))
get_twitter_data()

def periodic_work(interval):
  while True:
    get_twitter_data()
    time.sleep(interval)

periodic_work(60*0.1) 

