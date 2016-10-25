from flask import Flask, render_template, jsonify, request
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from key import access_key, secret_key
application = Flask(__name__)

host = ''
awsauth = AWS4Auth(access_key, secret_key, 'us-east-1', 'es')
                   
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
print(es.info())

@application.route('/', methods=["GET", "POST"])
def index():
    return render_template('googleMap.html')

@application.route('/queryES/<key>', methods=['POST'])
def queryES(key):
    res = es.search(index='indexgeo4', doc_type='twitter', size=1000, from_=0, body={"query":{'match':{"tweet": key}}})
    print res
    return jsonify(res['hits']['hits'])

@application.route('/queryClick/<clickCoordinates>', methods=['POST'])
def queryClick(clickCoordinates):
    coordinates = clickCoordinates.split(',')
    lat = coordinates[0]
    lon = coordinates[1]
    float(lat.replace(u'\N{MINUS SIGN}', '-'))
    float(lon.replace(u'\N{MINUS SIGN}', '-'))
    circleBody =  {"filter": {
                    "geo_distance": {
                      "distance": "100km", 
                      "location": { 
                        "lat": lat,
                        "lon": lon
                      }
                    }
                  }
                } 
    res = es.search(index='indexgeo4', doc_type='twitter', size=1000, from_=0, body=circleBody)
    
    # print res
    return jsonify(res['hits']['hits'])
    # return clickCoordinates

if __name__ == "__main__":
    application.run();
