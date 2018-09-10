# Import built-in libraries
import time
import os
import json

# Import installed libraries
import tweepy
import boto3

# Configure Logging
import logging
logger = logging.getLogger()
if os.getenv('LOG_LEVEL', '') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# Setting Environment Variables
# for local testing only
import yaml
with open('credentials.yml', 'r') as f:
    config = yaml.load(f)
for key, value in config.items():
    os.environ[key] = value


# Environment Variables
# twitter credentials
access_token = os.getenv('access_token', '')
access_token_secret = os.getenv('access_token_secret', '')
consumer_key = os.getenv('consumer_key', '')
consumer_secret = os.getenv('consumer_secret', '')
# TODO: (Optional) Change the following variables and run again
# track - a comma-separated list of phrases which will be used to determine what Tweets will be delivered on the stream.
track = os.getenv('track', 'datascience, dataengineering, deeplearning, cloudcomputing, bigdata, machinelearning')
# timeout in second
timeout = os.getenv('timeout', '20')
# TODO: Change the stream_name to your Firehose DeliveryStreamName
stream_name = os.getenv('stream_name', '<DeliveryStreamName>')


def twitter_authorizer():
    """Twitter API authentication.

    :param kwargs: Twitter API credentials.
    :return: Twitter API wrapper.
    """
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)


class TweetStreamListener(tweepy.StreamListener):
    """
    A simple Twitter Streaming API wrapper
    """
    def __init__(self, writer):
        logger.info('Initializing stream listener')
        super().__init__()
        self.counter = 0
        self.writer = writer

    def on_status(self, status):
        tweet = json.dumps(status._json) + '\n'
        self.writer.write(data=tweet)
        # tweet counter increased by 1
        self.counter += 1

    def on_error(self, status_code):
        logger.error('Connection error: {}'.format(status_code))
        # Return True to restart
        return True


class FirehoseWriter(object):
    """
    A simple Kinesis Firehose wrapper
    """
    def __init__(self, DeliveryStreamName):
        logger.info('Connecting to Kinesis firehose')
        self.firehose = boto3.client('firehose')
        if DeliveryStreamName in self.firehose.list_delivery_streams().get('DeliveryStreamNames', []):
            self.streamName = DeliveryStreamName
        else:
            raise ValueError('DeliveryStreamName not available')

    def write(self, data):
        response = self.firehose.put_record(
            DeliveryStreamName=self.streamName,
            Record={'Data': data},
        )
        logger.info('Status: {}\tText: {}'.format(response['ResponseMetadata']['HTTPStatusCode'], data[:100]))


if __name__ == '__main__':
    # Connect to Firehose
    writer = FirehoseWriter(DeliveryStreamName=stream_name)
    # Connect to Twitter API
    api = twitter_authorizer()
    listener = TweetStreamListener(writer=writer)
    streamer = tweepy.Stream(auth=api.auth,
                             listener=listener,
                             retry_count=3)
    # Start streaming
    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=[track],
                    languages=['en'],
                    async=True)
    # Stop streaming after timeout seconds
    time.sleep(int(timeout))
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
    logger.info('Total tweets received in {} seconds: {}'.format(timeout, listener.counter))
