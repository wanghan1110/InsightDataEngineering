import random
import sys
import six
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        lon = random.randint(110,111)
        lat = random.randint(13,14)
        while True:
            time_field = datetime.now().strftime("%Y%m%d-%H%M%S")
            lon += random.randint(-1, 1)/100.0
            lat += random.randint(-1, 1)/100.0
            str_fmt = "{},{}"
            message_info = str_fmt.format(lon,lat)
            print message_info
            self.producer.send_messages('drone_data_new', source_symbol, message_info)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 