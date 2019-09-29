import redis
from WatchDogs_MongoWrapper import MongoWrapper
import json
import traceback

class RedisWrapper():
    def __init__(self, decrypt_key):
        self.redicclient = redis.StrictRedis(host='35.236.16.13', port=6379, db=0, decode_responses=True)
        self.mng = MongoWrapper(decrypt_key)

    def get_logger(self, logger_name):
        return self.mng.get_logger(logger_name)

    def redis_update_json(self, api_string, key):
        if api_string == 'get_tweets_with_lat_long/':

            json_data = self.mng.get_tweets_with_lat_long(key)
            self.redicclient.execute_command('JSON.SET', api_string+key, '.', json_data)

        elif api_string == 'get_polarity_tweets_of_stock/':

            json_data = self.mng.get_polarity_tweets_of_stock(key)
            self.redicclient.execute_command('JSON.SET', api_string + key, '.', json_data)


    def redis_insert_tweet(self, key, tweet):
        """
        Either insert a single tweet or multiple tweets and this def will update the redis cache accordingly
        :param key:
        :param tweets:
        :return:
        """
        try:
            lat_long_list = tweet['Geo']['coordinates']
            has_lat_long = True
        except:
            has_lat_long = False
            lat_long_list = ['None', 'None']

        if has_lat_long:
            sentiment_value = float(tweet["Sentiment_Value"])
            full_text = tweet["Text"]
            root_json_path = {}
            root_json_path["Latitude"] = lat_long_list[0]
            root_json_path["Longitude"] = lat_long_list[1]
            root_json_path["Tweet_Text"] = full_text
            root_json_path["Sentiment_Value"] = sentiment_value
            api_string = 'get_tweets_with_lat_long/'
            self.redicclient.execute_command('JSON.ARRAPPEND', api_string+key, '.', json.dumps(root_json_path))

        sentiment_polarity = int(tweet["Sentiment_Polarity"])
        full_text = tweet["Text"]
        root_json_path = {}
        root_json_path["Latitude"] = lat_long_list[0]
        root_json_path["Longitude"] = lat_long_list[1]
        root_json_path["Tweet_Text"] = full_text
        root_json_path["Sentiment_Polarity"] = sentiment_polarity
        api_string = 'get_polarity_tweets_of_stock/'
        if sentiment_polarity == -1:
            root_path = '.Negative_Tweets'
        elif sentiment_polarity == 0:
            root_path = '.Neutral_Tweets'
        elif sentiment_polarity == 1:
            root_path = '.Positive_Tweets'
        self.redicclient.execute_command('JSON.ARRAPPEND', api_string + key, root_path, json.dumps(root_json_path))

    def redis_get_json(self, api_string, key):
        return self.redicclient.execute_command('JSON.GET', api_string+key)

    def redis_flush_all(self):
        """
        Danger. This flushes the whole DB. Careful
        :return:
        """
        self.redicclient.flushdb()

