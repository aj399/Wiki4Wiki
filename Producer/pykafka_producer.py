import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
from datetime import date
import string
import random
import time
import json


def producerFunc(singlewindow=False ):
    with open('../myConfig.json', 'r') as f:
        myconfigs = json.load(f)
    # Set up the kafka clients. Here I have used 30 partitions and I am defining 3 working nodes as clients
    # Use hashed partitioner for balanced load distribution
    client   = KafkaClient(hosts=str(myconfigs["WORKERS_IP"]),zookeeper_hosts=str(myconfigs["MASTER_IP"]))
    topic    = client.topics[str(myconfigs["TOPIC"])]
    hash_partitioner = HashingPartitioner()
    producer = topic.get_producer(partitioner=hash_partitioner, linger_ms = 200)
    starttime = 0
    if singlewindow==True:
        starttime = produceFun(producer)
        #print starttime
    else:
        while True:
            starttime = produceFun(producer)
            #print starttime


def produceFun(producer):
    currItem = random.choice(string.letters)
    dTime = date.today().ctime()
    print currItem, dTime
    outputStr = "{};{}".format(currItem, dTime) 
    producer.produce(outputStr, partition_key=str(currItem))
    
if __name__ == '__main__':
    #process(sys.argv[1:])
    producerFunc()
    sys.exit(0)