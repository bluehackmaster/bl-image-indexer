import numpy as np
import time
import faiss
import signal
import sys
from util import s3
from threading import Timer

import tensorflow as tf
import json
import redis
import os
import logging

from magi import feature_extract
from bluelens_spawning_pool import spawning_pool

STR_BUCKET = "bucket"
STR_STORAGE = "storage"
STR_CLASS_CODE = "class_code"
STR_NAME = "name"
STR_FORMAT = "format"

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
# DATA_SOURCE = os.environ['DATA_SOURCE']
DATA_SOURCE_QUEUE = 'REDIS_QUEUE'
DATA_SOURCE_DB = 'DB'

REDIS_IMAGE_FEATURE_QUEUE = 'bl:image:feature:queue'
REDIS_IMAGE_INDEX_QUEUE = 'bl:image:index:queue'

rconn = redis.StrictRedis(REDIS_SERVER)
logging.basicConfig(filename='./log/main.log', level=logging.DEBUG)
rconn = redis.StrictRedis(REDIS_SERVER, port=6379)
feature_extractor = feature_extract.Feature()

heart_bit = True

def job():
  logging.debug('start')
  def items():
    while True:
      yield rconn.blpop([REDIS_IMAGE_INDEX_QUEUE])

  def request_stop(signum, frame):
    print('stopping')
    rconn.connection_pool.disconnect()
    print('connection closed')

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)

  for item in items():
    key, image_data = item
    print(image_data)
    logging.debug(image_data)
    if type(image_data) is str:
      image_info = json.loads(image_data)
    elif type(image_data) is bytes:
      image_info = json.loads(image_data.decode('utf-8'))

    file = download_image(image_info)
    feature = feature_extractor.extract(file)
    image_info['feature'] = feature.tolist()
    rconn.lpush(REDIS_IMAGE_FEATURE_QUEUE, json.dumps(image_info))

    global  heart_bit
    heart_bit = True

def check_health():
  print('check_health: ' + str(heart_bit))
  logging.debug('check_health: ' + str(heart_bit))
  global  heart_bit
  if heart_bit == True:
    heart_bit = False
    Timer(60, check_health, ()).start()
  else:
    exit()

def exit():
  print('exit: ' + SPAWN_ID)
  logging.debug('exit: ' + SPAWN_ID)
  data = {}
  data['namespace'] = 'index'
  data['id'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.delete(data)

def download_image(image_info):
  TMP_CROP_IMG_FILE = './tmp.jpg'
  storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
  key = os.path.join(image_info[STR_CLASS_CODE], image_info[STR_NAME]+ '.' + image_info[STR_FORMAT])
  storage.download_file_from_bucket(image_info[STR_BUCKET], TMP_CROP_IMG_FILE, key)
  return TMP_CROP_IMG_FILE

if __name__ == "__main__":
  Timer(60, check_health, ()).start()
  job()
