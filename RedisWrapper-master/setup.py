from setuptools import setup

setup(
 name='WatchDogs_RedisWrapper',    # This is the name of your PyPI-package.
 packages=['WatchDogs_RedisWrapper'],
install_requires=[
    'pymongo',
    'textblob',
    'pandas',
    'python-logstash',
    'redis'
    ],
 version='2.0',

)