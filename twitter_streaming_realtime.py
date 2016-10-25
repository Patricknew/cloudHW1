#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream 
import json
import time

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

# AWS host
host = ""

# Connect to AWS
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
awsauth = AWS4Auth(AWS_ACCESS_KEY_ID,
                   AWS_SECRET_ACCESS_KEY,
                   "us-east-1",
                   "es")
                   
# Elasticsearch client
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
mappings = {"mappings":{
                "twitter": {
                    "properties": {
                         "tweet":  {
                            "type": "string"
                         },
                         "location": {
                             "type": "geo_point"
                         }}
                }
            }}
es.indices.create(index='indexgeo5', body=mappings)                   

#Variables that contains the user credentials to access Twitter API 
access_token = ''
access_token_secret = ''
consumer_key = ''
consumer_secret = ''


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_status(self, status):
	#print status._json
        if status.coordinates != None:
            
            print status._json
            coord = status.coordinates

            es_entries = { 'tweet': status.text,
                   'location': str(coord['coordinates'][1])+","+str(coord['coordinates'][0])
    }
            es.index(index="indexgeo5", doc_type="twitter", body=es_entries)    
            with open("tweet_data.txt", "a") as text_file:
    	    	    text_file.write(json.dumps(status._json))
    	    	    text_file.write('\n')            
        return True        

    
    def on_error(self, status):
        print status, type(status)
        if status == 420:
            time.sleep(10)
        return True
            


def startStream():
    try:
	#This handles Twitter authetification and the connection to Twitter Streaming API
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)

	#This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
	filterlist = ['Trump', 'election', 'love', 'debate', 'Hillary', 'game', 'apple', 'fun', 'hashtag', 'music', 'sports', 'food']
	stream.filter(track=filterlist)
    except:
	startStream()


if __name__ == '__main__':
	startStream()
