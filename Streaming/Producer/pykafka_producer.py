#Generating random pagerequests from 6 million possible wikipedia articles present in 'wikiList' file 

import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
from datetime import date
import time
import string
from random import randint
import time
import json


def producerInit():
    wikiArt = {}
    nArt = 0
    with open('../streamingConfig.json', 'r') as cFile:
        myconfigs = json.load(cFile)

    wFile = open('../wikiList', 'r')
    for line in wFile:
	#Extracting wikipedia article name which is present in the 2 word in each line
        wikiArt[nArt] = line.split(" ")[1].lower()
        nArt += 1
       
    nArt -= 1
    client   = KafkaClient(hosts=str(myconfigs["WORKERS_IP"]),zookeeper_hosts=str(myconfigs["MASTER_IP"]))
    topic    = client.topics[str(myconfigs["CONSUMER_TOPIC"])]
    hash_partitioner = HashingPartitioner()
    producer = topic.get_producer(partitioner=hash_partitioner, linger_ms = 200)
    starttime = 0
    produceFunc(producer, wikiArt, nArt)


def produceFunc(producer, wikiArt, nArt):
    while True:
		dTime = time.time()
		currArticle = wikiArt[randint(0,nArt-1)]
		outputStr = "{}\t{}".format(currArticle, np.int64(np.floor(dTime))) 
		#sending page requests keyed by the wikipedia article so that same article always goes to the same flink consumer, 
		#so there is no need for shuffling to aggregate 10 minute of the pagerequest data
                producer.produce(outputStr, partition_key=str(currArticle))
    
if __name__ == '__main__':
    producerInit()
    sys.exit(0)
