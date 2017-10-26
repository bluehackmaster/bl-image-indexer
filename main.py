import numpy as np
import time
import faiss
import signal
import sys
from util import s3

import tensorflow as tf
import json
import redis
import os
from redis import Redis
from os import listdir
from os.path import isfile, join
IMG_NUM = 1408
QUERY_IMG = 22
CANDIDATES = 5

STR_BUCKET = "_bucket"
STR_STORAGE = "_storage"
STR_CLASS_CODE = "_class_code"
STR_NAME = "_name"
STR_FORMAT = "_format"

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

CLASSIFY_GRAPH = os.environ['CLASSIFY_GRAPH']
REDIS_SERVER = os.environ['REDIS_SERVER']

rconn = redis.StrictRedis(REDIS_SERVER)


REDIS_KEY_IMAGE_INFO = 'image_info'
REDIS_KEY_IMAGE_QUEUE = 'image_queue'
REDIS_KEY_IMAGE_INDEX = 'image_index'

def get_image_info(id):
  data = rconn.hget(REDIS_KEY_IMAGE_INFO, id)
  image_info =json.loads(data)
  # print(image_info)
    # image_info = {}
    # image_info[STR_STORAGE] =
    # image_info[STR_BUCKET] = rconn.hget(KEY_IMAGE_INFO, 'bucket')
    # image_info[STR_CLASS_CODE] = rconn.hget(KEY_IMAGE_INFO, 'class_code')
    # image_info[STR_NAME] = rconn.hget(KEY_IMAGE_INFO, 'name')
    # image_info[STR_FORMAT] = rconn.hget(KEY_IMAGE_INFO, 'format')
  return image_info

def download_image(image_info):
  TMP_CROP_IMG_FILE = './tmp.jpg'
  storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
  key = os.path.join(image_info[STR_CLASS_CODE], image_info[STR_NAME]+ '.' + image_info[STR_FORMAT])
  storage.download_file_from_bucket(image_info[STR_BUCKET], TMP_CROP_IMG_FILE, key)
  return TMP_CROP_IMG_FILE

def get_image(id):
  image_info = get_image_info(id)
  file = download_image(image_info)
  return file

# def get_files():
#     return [f for f in listdir(join(DATA_DIR, FOLDER)) if isfile(join(DATA_DIR, FOLDER, f))]

def main():
  stop_requested = False
  rconn.delete(REDIS_KEY_IMAGE_INDEX)

  p = rconn.pubsub()
  p.subscribe('crop/done')
  with tf.gfile.FastGFile(CLASSIFY_GRAPH, 'rb') as f:
      graph_def = tf.GraphDef()
      graph_def.ParseFromString(f.read())
      _ = tf.import_graph_def(graph_def, name='')

  with tf.Session() as sess:
      pool3 = sess.graph.get_tensor_by_name('pool_3:0')

  def items():
    while True:
      yield rconn.blpop([REDIS_KEY_IMAGE_QUEUE])


  def request_stop(signum, frame):
    print 'stopping'
    stop_requested = True
    rconn.connection_pool.disconnect()
    print 'connection closed'

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)

  index = faiss.IndexFlatL2(2048)
  index2 = faiss.IndexIDMap(index)
  for item in items():
    features = []
    key, image_id = item
    f = get_image(image_id)
    with tf.gfile.GFile(f, 'rb') as fid:
      image_data = fid.read()
      pool3_features = sess.run(pool3,{'DecodeJpeg/contents:0': image_data})
      feature = np.squeeze(pool3_features)
      features.append(feature)
      xb = np.array(features)

      s = time.time()
      index = faiss.IndexFlatL2(xb.shape[1])
      rconn.lpush(REDIS_KEY_IMAGE_INDEX, image_id)
      idx = rconn.llen(REDIS_KEY_IMAGE_INDEX)
      print(idx)
      ids = np.array([idx])
      index2 = faiss.IndexIDMap(index)
      index2.add_with_ids(xb, ids)
      check_redis_sub(p, index2)


def check_redis_sub(pubsub, index):
  message = pubsub.get_message()
  if message:
    command = message['data']
    if command == b'DONE':
      print("==== Completed indexing ===")
      faiss.write_index(index, 'faiss.index')

# Evaluate
def evaluate(arr1, arr2):
    top_1 = (arr1[:,0] == arr2[:,0]).sum() / arr1.shape[0]
    total = 0
    for t in np.c_[arr1, arr2]:
        _, cnt = np.unique(t, return_counts=True)
        total += (cnt >= 2).sum()
    top_k = total / arr1.shape[0] / arr1.shape[1]
    print('recall@1: {:.2f}, top {} recall: {:.2f}'.format(top_1, arr1.shape[1], top_k))

if __name__ == "__main__":
    main()
