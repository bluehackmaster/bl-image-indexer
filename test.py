import redis
import signal
import sys
from time import sleep

stop_requested = False
def main():
    rconn = redis.StrictRedis('127.0.0.1')

    def items():
        while True:
            print(rconn.hget('image_info', 'bucket'))
            yield rconn.blpop(['image_queue'])

    def request_stop(signum, frame):
        print 'stopping'
        stop_requested = True
        rconn.connection_pool.disconnect()
        print 'connection closed'

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    for item in items():
      key, value = item
      print(key)
      print(value)
      print 'processed item'
      if stop_requested:
          sys.exit()


if __name__ == '__main__':
    main()