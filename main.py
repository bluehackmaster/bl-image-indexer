import numpy as np
import time
import faiss
from helper import *
from util import s3

import tensorflow as tf
import json
import os
from os import listdir
from os.path import isfile, join
IMG_NUM = 1408
QUERY_IMG = 22
CANDIDATES = 5

STR_BUCKET = "bucket"
STR_STORAGE = "storage"
STR_CLASS_CODE = "class_code"
STR_NAME = "name"
STR_FORMAT = "format"

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

CLASSIFY_GRAPH = os.environ['CLASSIFY_GRAPH']

def get_image_info():
    # ToDo: BRPOP image ID from 'image_queue' in the 'bl-mem-store'

    # ToDo: Get image info from 'image_info' in the 'bl-mem-store'

    # These are hardcoded.
    image_info = {}
    image_info[STR_STORAGE] = 's3'
    image_info[STR_BUCKET] = 'bluelens-style-object'
    image_info[STR_CLASS_CODE] = 'n0100016'
    image_info[STR_NAME] = '59e194880eb291000ff42a74'
    image_info[STR_FORMAT] = 'jpg'
    return image_info

def download_image(image_info):
  TMP_CROP_IMG_FILE = './tmp.jpg'
  storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
  key = os.path.join(image_info[STR_CLASS_CODE], image_info[STR_NAME]+ '.' + image_info[STR_FORMAT])
  storage.download_file_from_bucket(image_info[STR_BUCKET], TMP_CROP_IMG_FILE, key)
  return TMP_CROP_IMG_FILE

def get_image():
  image_info = get_image_info()
  file = download_image(image_info)
  return file

# def get_files():
#     return [f for f in listdir(join(DATA_DIR, FOLDER)) if isfile(join(DATA_DIR, FOLDER, f))]

def main():
  with tf.gfile.FastGFile(CLASSIFY_GRAPH, 'rb') as f:
      graph_def = tf.GraphDef()
      graph_def.ParseFromString(f.read())
      _ = tf.import_graph_def(graph_def, name='')

  with tf.Session() as sess:
      pool3 = sess.graph.get_tensor_by_name('pool_3:0')
      features = []

  # while True:
  f = get_image()
  with tf.gfile.GFile(f, 'rb') as fid:
    image_data = fid.read()
    pool3_features = sess.run(pool3,{'DecodeJpeg/contents:0': image_data})
    feature = np.squeeze(pool3_features)
    print(feature)
    features.append(feature)

    xb = np.array(features)

    #print(xb)
    nq = 5
    xq = np.copy(xb[:nq])
    nb, d = xb.shape
    n_candidates = 10

    # Index (faiss)
    s = time.time()
    index = faiss.IndexFlatL2(xb.shape[1])
    ids = np.arange(xb.shape[0])
    index2 = faiss.IndexIDMap(index)
    print(index.is_trained)
    print(xb)
    print(index.ntotal)
    #index.add(xb)
    index2.add_with_ids(xb, ids)
    print(index.ntotal)
    print('Index time (faiss): {:.2f} [ms]'.format((time.time() - s) * 1000))

    faiss.write_index(index2, 'faiss.index')

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
