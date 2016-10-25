import json
import pandas as pd
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

# Read in file
tweets_data = []
with open("tweet_data.txt", "r") as tweetfile:
    for line in tweetfile.readlines():
        tweet = json.loads(line)
        tweets_data.append(tweet)
        
tweets = pd.DataFrame()
tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
tweets['location'] = map(lambda tweet: tweet['coordinates']['coordinates'], tweets_data)



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



es.indices.create(index='indexgeo4', body=mappings)

for i in range(len(tweets)):
    es_entries = { 'tweet': tweets['text'][i],
                   'location': str(tweets['location'][i][1])+","+str(tweets['location'][i][0])
    }
    es.index(index="indexgeo4", doc_type="twitter", body=es_entries)
