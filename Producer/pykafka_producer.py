import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
import time
import json


def producerf(singlewindow=True ):
    tw = 1*60. # The time window in seconds
    nvfv = {'1'   :[0       , int(10), int(1)], # [0,   1e6): (forgetters)
            '2'   :[int(10), int(20), int(1)], # [1e6, 2e6): (login-logout) 
            '3'  :[int(20), int(30), int(1)], # [2e6, 3e6): (active-user)
            '40' :[int(30), int(40), int(1)]} # [3e6, 4e6): (machine-spam)
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
        starttime = producef(tw, nvfv, producer, starttime)
        print starttime
    else:
        while True:
            starttime = producef(tw, nvfv, producer, starttime)
            print starttime


def producef(tw, nvfv, producer, starttime):
    dtime = 0.
    nv = 0 # Initiate the number of the visits to the website during the time window
    ld = []
    for v in nvfv:
        nv += int(v)*nvfv[v][2]
        ld.append(np.repeat(np.random.randint(nvfv[v][0], nvfv[v][1], size=nvfv[v][2]), int(v))) 

    ld1 = np.concatenate(ld[:])
    np.random.shuffle(ld1)
    dt = tw/len(ld1)
    eventTime = 0
    delay = 0
    timedd = time.time()
    #times = time.time()
    for index, item in enumerate(ld1):
        times = time.time()
        eventTime  += dt-delay # event time in Seconds
        currID = item
        print currID, eventTime
        outputStr = "{};{}".format(currID, np.int64(np.floor(eventTime+starttime)*1e3)) # write the time in milliseconds
        producer.produce(outputStr, partition_key=str(currID))
        delay = (time.time()-times)
        time.sleep(max(0.7*(dt-delay),1e-9))
    times = time.time()
    print time.time()-timedd
    return starttime + tw
    
if __name__ == '__main__':
    #process(sys.argv[1:])
    producerf()
    sys.exit(0)