# Import built-in libraries
import os
import json

# Import installed libraries
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws_sign import AWSV4Sign
from elasticsearch.helpers import bulk

# Configure Logging
import logging
logger = logging.getLogger()
if os.getenv('LOG_LEVEL', '') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Environment Variables
region_name = os.getenv('region_name')
es_endpoint = os.getenv('es_endpoint')
upload = False
get_sentiment = False


class ESPipeline(object):
    es_index = 'tweet-index'
    doc_type = 'tweet-type'
    mapping = {
        doc_type: {
            'properties': {
                'id_str'
                'created_at': {'type': 'text'},
                'timestamp_ms': {'type': 'date'},
                'text': {'type': 'text'},
                'user': {'type': 'keyword'},
                'sentiment': {
                    'properties': {
                        'Sentiment': {'type': 'keyword'},
                        'SentimentScore': {
                            'properties': {
                                'Positive': {'type': 'float'},
                                'Negative': {'type': 'float'},
                                'Neutral': {'type': 'float'},
                                'Mixed': {'type': 'float'}
                            }
                        }
                    }
                },
                'hashtags': {'type': 'keyword'},
                'mentions': {'type': 'keyword'},
                'coordinates': {'type': 'geo_point'},
                'city': {'type': 'keyword'},
            }
        }
    }

    def __init__(self, upload=False, get_sentiment=False):
        # Connect to s3
        self.s3 = boto3.client(service_name='s3')
        # Establish credentials
        session_var = boto3.session.Session()
        credentials = session_var.get_credentials()
        awsauth = AWSV4Sign(credentials, region_name, 'es')
        # Connect to es
        self.es = Elasticsearch(host=es_endpoint,
                                port=443,
                                connection_class=RequestsHttpConnection,
                                http_auth=awsauth,
                                use_ssl=True,
                                verify_ssl=True)
        self.upload = upload
        if self.upload and not self.es.indices.exists(index=self.es_index):
            logger.info('Creating mapping...')
            self.es.indices.create(index=self.es_index,
                                   body={'mappings': self.mapping})

        if get_sentiment:
            self.comprehend = boto3.client(service_name='comprehend')
        else:
            self.comprehend = None

    @staticmethod
    def json_parser(tweets):
        """A json parser wrapper

        :param raw_data: json object
        :return: list of Python dictionary
        """
        for tweet in tweets.strip().split('\n'):
            yield json.loads(tweet)

    def get_sentiment(self, text):
        """AWS Comprehend Sentiment Analysis Wrapper

        :param text: input text
        :param kwargs:
        :return:
        """
        if self.comprehend:
            sentiment = self.comprehend.detect_sentiment(Text=text, LanguageCode='en')
            return {k: v for k, v in sentiment.items() if k in ('Sentiment', 'SentimentScore',)}
        else:
            return {}

    def tweet_extractor(self, tweet):
        # extract hashtags and user_mentions
        hashtags = [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']]
        mentions = [user_mention['screen_name'].lower() for user_mention in tweet['entities']['user_mentions']]
        # extracted data
        data = {'id_str': tweet['id_str'],
                'created_at': tweet['created_at'],
                'timestamp_ms': tweet['timestamp_ms'],
                'text': tweet['text'],
                'hashtags': hashtags,
                'mentions': mentions,
                'user': tweet['user']['screen_name']}
        # get geo-coordinates if available
        if tweet.get('geo') is not None:
            data['coordinates'] = tweet['geo'].get('coordinates')[::-1]
        # get city if available
        if tweet.get('place') is not None and tweet['place'].get('place_type') == 'city':
            data['city'] = tweet['place'].get('full_name')
        # change text/hashtags/user_mentions if extend_tweet is available
        if tweet.get('extended_tweet'):
            extended_tweet = tweet['extended_tweet']
            if extended_tweet.get('full_text'):
                data['text'] = extended_tweet['full_text']
            hashtags = [hashtag['text'].lower() for hashtag in extended_tweet['entities']['hashtags']]
            if hashtags:
                data['hashtags'] = hashtags
            mentions = [user_mention['screen_name'].lower() for user_mention in
                        extended_tweet['entities']['user_mentions']]
            if mentions:
                data['mentions'] = mentions
        # run sentiment function
        if self.comprehend:
            data['sentiment'] = self.get_sentiment(text=data['text'])
        # output extracted data
        return data

    def run(self, bucket, key, bulk_size=25):
        tweets = []
        bulk_size, count = bulk_size, 0
        body = self.s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
        for tweet in self.json_parser(tweets=body):
            parsed_tweet = self.tweet_extractor(tweet=tweet)
            if self.upload:
                bulk_doc = {
                    "_index": self.es_index,
                    "_type": self.doc_type,
                    "_id": parsed_tweet['id_str'],
                    "_source": parsed_tweet
                }
                tweets.append(bulk_doc)
                if len(tweets) == bulk_size:
                    success, _ = bulk(self.es, tweets)
                    count += success
                    logger.info('ElasticSearch indexed {} documents'.format(count))
                    tweets = []
            else:
                print(parsed_tweet)
        else:
            if self.upload:
                success, _ = bulk(self.es, tweets)
                count += success
                logger.info('ElasticSearch indexed {} documents'.format(count))
        return count


def lambda_handler(event, context):
    pipeline = ESPipeline(upload=upload, get_sentiment=get_sentiment)
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        total = pipeline.run(bucket=bucket, key=key)
        logger.info('Total tweets uploaded: {}'.format(total))
    return event
