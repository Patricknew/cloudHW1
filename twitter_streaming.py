#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream 
import json
import time

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
