# Import Libraries
import time
import os

# Import install libraries
import tweepy

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
# track
# A comma-separated list of phrases which will be used to determine what Tweets will be delivered on the stream.
# A phrase may be one or more terms separated by spaces, and a phrase will match if all of the terms in the phrase are
# present in the Tweet, regardless of order and ignoring case.
# By this model, you can think of commas as logical ORs, while spaces are equivalent to logical ANDs
# (e.g. ‘the twitter’ is the AND twitter, and ‘the,twitter’ is the OR twitter).
track = os.getenv('track', 'datascience, dataengineering, deeplearning, cloudcomputing, bigdata, machinelearning')
# timeout in second
timeout = os.getenv('timeout', '20')


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
    def __init__(self):
        logger.info('Initializing stream listener')
        super().__init__()
        self.counter = 0

    def on_connect(self):
        logger.info('Stream Listener connected.')

    def on_status(self, status):
        # status is a instance of tweepy.Status class, status._json is the status data formatted as a python dict
        # print(status._json)
        # Ref: https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        print('\nCreated at: {}'.format(status.created_at))
        print(status.text)
        # tweet counter increased by 1
        self.counter += 1

    def on_error(self, status_code):
        logger.error('Connection error: {}'.format(status_code))
        # Return True to restart
        return True


if __name__ == '__main__':
    # Connect to Twitter API
    api = twitter_authorizer()
    listener = TweetStreamListener()
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
