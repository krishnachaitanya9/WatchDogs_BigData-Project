from WatchDogs_RedisWrapper.redis_wrapper import RedisWrapper
import unittest
import redis
import json

class RedisWrapperTests(unittest.TestCase):
    def test_push(self):
        redis_connection = RedisWrapper()
        api_string = 'get_tweets_with_lat_long/'
        key = 'Facebook'
        redis_connection.redis_update_json(api_string, key)

    def test_push2(self):
        redis_connection = RedisWrapper()
        api_string = 'get_polarity_tweets_of_stock/'
        key = 'Facebook'
        redis_connection.redis_update_json(api_string, key)

    def test_pull(self):
        redis_connection = RedisWrapper()
        api_string = 'get_tweets_with_lat_long/'
        key = 'Facebook'
        json_string = redis_connection.redis_get_json(api_string, key)
        decoded_string = json_string.decode("utf-8")
        print(decoded_string)

    def test_pull2(self):
        redis_connection = RedisWrapper()
        api_string = 'get_polarity_tweets_of_stock/'
        key = 'Facebook'
        json_string = redis_connection.redis_get_json(api_string, key)
        print(json_string)

    def test_insert_tweet(self):
        redis_connection = RedisWrapper()
        api_string = 'get_polarity_tweets_of_stock/'
        key = 'TestStock'
        data = {
            'foo' : 'bar',
            'test': [{
                'a' : 5,
                'b' : 6
            }]

        }
        redicclient = redis.StrictRedis(host='35.236.16.13', port=6379, db=0, decode_responses=True)
        redicclient.execute_command('JSON.SET', api_string + key, '.', json.dumps(data))

        data = {
                'a' : 8,
                'b' : 9
            }
        redicclient.execute_command('JSON.ARRAPPEND', api_string + key, '.test', json.dumps(data))
        print(redis_connection.redis_get_json(api_string, key))
        redis_connection.redis_flush_all()


if __name__ == '__main__':
    unittest.main()