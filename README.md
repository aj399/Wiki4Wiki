# wiki4wiki

# Table of Contents
1. [Summary](README.md#summary)
2. [PipeLine](README.md#pipeline)
3. [Dependencies](README.md#dependency)
4. [Installation](README.md#installation)
5. [Slides](README.md#slides)

# Summary
  My application discovers anomalies in page requests of Wikipedia articles , by juxtaposing 7 months of historical data with the current streaming data. Every minute it detects the articles that are trending in the last 1 hour among 6 million possible articles using flink. And every hour a spark job identifies the articles with anomalous page requests pattern by juxtaposing the aggregated streaming data and the historical average my measuring the difference in behaviour using canberra distance(https://en.wikipedia.org/wiki/Canberra_distance).

# Pipeline
Python producers generate random request for 6 million possible articles and that data is send to flink.

Flink creates a sliding window of ten minute width updated every one minute and page request for each article in aggreagated in those windows. The window is named based on the starting minute of the window(10:00 - window 0, 11:59  - window 59 ..). These data is written to one of 10 possible kafka topics(0-9) based on the unit's digits of window(window 0- topic 0, window 59 - topic 9 ..). These data is read again by flink sliding window of width one hour updated every one minute. That is each window would have page requests count for each of these articles for 6 consectuive 10 minute windows which constitutes the past 1 hour(eg: topic 1 could have data from 6 windows -  window 1(10:01-10:11), window 11(10:11-10:21), window 21(10:21-10:31), window 31(10:11-10:21), window 41(10:41-10:51), window 51(10:51-11:01)). Flink calculates the trending articles by filtering out articles whose page requests has gone down in one of the those 6 consecutive windows. The result is sent to redis and from there data is queried by flask to display the trending articles.  

Flink also at end of each hour sends aggregated page request for each article for one hour and send that data to S3. S3 also contains average hourly page request of each article calculated by aggregating 7 months of data(1 TB) using a spark job. Cron at every fifth minute of the hour triggers a spark job called canbera.py. The spark job gathers the current houlry page request for the article and average houlry page request for the article till the current hour and represent both of them as vectors. 

historical vector = (historical average at hour 1, historical average at hour 2, .., historical average at hour 11)  
current vector = (current requests at hour 1, current requests at hour 2, .., current requests at hour 11)  

calculates the distance between them using canberra distance(https://en.wikipedia.org/wiki/Canberra_distance) and identifies the articles with top 5 distance and send that data to redis.

Also at the end of day spark job is triggered by cron which recalculates the historical average with the new days data using cumulative moving average.

![alt text](https://github.com/aj399/Wiki4Wiki/blob/master/pipeline.PNG "PipeLine")

# Dependency

This program requires:

1. Python version 2.7
2. Java version 1.8
3. Apache Flink version 1.2.1
4. Apache ZooKeeper version 3.4.9
5. Apache Kafka version 0.9.0.1
6. Redis version 3.2.6
7. PyKafka
8. Tornado Server 

# Installation
Clone the repository using git clone https://github.com/aj399/Wiki4Wiki.git

Install all the above mentioned dependencies 

## Stream Job

1. Create the streamingConfig.json inside the Streaming folder

2. Assign the following values to 'streamConfig.json':
  {
    
    "WORKERS_IP": 'flink worker ips':9092,

    "MASTER_IP" : 'flink master ip':2181,

    "CONSUMER_TOPIC" : 'topic name to which python producer publishes the article request',

    "PRODUCER_TOPIC" : 'base topic name of the diffenet queues(queue0 - queue9) flink produces,

    "REDIS_IP" : 'Redis ip address'
 }

3. Run the producer( You can run 2 or 3 producers depending on throughput you require, you could also provide a real life stream if you have it) in the 'Stream' folder:

    python /Streaming/Producer pykafka_producer.py 

4. Clean The maven repository which is inside 'kafkaFlink' folder:

    mvn clean package

5. Build the repository which is inside 'kafkaFlink' folder:

    mvn install

6. Run the flink job 'MainStream'

    /usr/local/flink/bin/flink run -c consumer.MainStream target/consumer-0.0.jar

## Batch Job

1. Create the batchConfig.json inside the Batch folder

2. Assign the following values to 'batchConfig.json':
  {

    "APP_NAME": 'Name of the spark job',

    "S3_ACCESS_KEY": 'Acces key of your s3 bucket',

    "S3_SECRET_KEY": 'Secret key of your s3 bucket',

    "BUCKET_NAME": 'Name of your s3 bucket',

    "OP_FOLDER_NAME": 'Permanent Folder were hourly averages of articles are stored',

    "IP_FOLDER_NAME": 'Permanent folder where historical data of hourly log data is stored',

    "WORK_FOLDER_NAME": 'Temporary folder where daily hourly log data is stored',

    "TEMP_FOLDER_NAME": 'Temporary folder where you store your temporary results',

    "REDIS_IP": "Redis ip address"
  }

3. Calculate the historical averages(Call it only once on your entire historical data) by calling 'btachAvgArtView.py':

    $SPARK_HOME/bin/spark-submit --master spark://'ip address of your spark master':7077 batchAvgArtView.py

4. Run the anomaly detection job every hour when new hourly data has arrived on s3 from flink, you could use cron or preferrably luigi(using cron now which is triggered every hour, which is not working as flink is not writing to s3 in the expected time limit, hoping to to trigger it using luigi in future)

## Web

1. Create the webConfig.json inside the Web folder

2. Assign the followin values to 'webConfig.json':
  {

    "REDIS_IP": ' Redis ip address',

    "TRENDING": 'key of the trending topics
  }

3. Run the web app inside the 'web' folder using tornado

    sudo -E python Web/tornadoapp.py

# Slides
http://bit.ly/2uRSQS8

