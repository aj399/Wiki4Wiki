# Wiki4Wiki

# Table of Contents
1. [Summary](README.md#summary)
2. [PipeLine](README.md#pipeline)
3. [Dependencies](README.md#dependency)
4. [Installation](README.md#installation)
5. [Slides](README.md#slides)

# Summary
My application discovers anomalies in page requests of Wikipedia articles, by juxtaposing 7 months of historical data with the current streaming data and identifying which articles' pageviews are most divergent. Every minute it detects the articles that are trending in the last 1 hour among 6 million possible articles using Flink. At every hour, a Spark job identifies the articles with anomalous page requests pattern by juxtaposing the aggregated streaming data and historical average, and then measuring the difference in behaviour using a concept called canberra distance (https://en.wikipedia.org/wiki/Canberra_distance).

# Pipeline
Python producers generate random request for 6 million possible articles and sends the data to Kafka, which Flink then pulls from.

Every minute, Flink aggregates 10 minutes worth of pageveiws for each article into one sliding window. This data is then published to one of 10 possible Kafka topics (0-9) depending on the last digit of the starting minute in any particular window. For instance, if there were 100 pageviews made for one particular article during the sliding window from 10:00a.m. to 10:10a.m., that data would be written to Kafka Topic 0. The number of pageviews for that same article during the sliding window from 10:59a.m. to 11:09 would correspondingly be written to Kafka Topic 9.

Using this scheme, we can determine how many pageviews have been made during any particular hour by grabbing six consecutive sliding windows.

At every minute that the Flink consumer is triggered, it checks the corresponding topic and pulls the last six sliding windows. For instance, at 10:00a.m., pulling the last six sliding windows from Kafka Topic 0 would result in six aggregated pageview values between 9:00a.m. and 10:00a.m.

Articles are then identified as "trending" when the number of pagviews has not decreased in any of those six consecutive windows. That data is saved to Redis and available to the Flask front-end.  

Additionally, at end of each hour the aggregated pageviews for each article over the past one hour is saved to S3. S3 also contains average hourly page request of each article calculated by aggregating 7 months of data (1 TB) using a Spark job. A cron job is then triggered at every fifth minute of the hour to run a Spark job that calculates the canbera distance between the historical and streaming data, representing both as vectors. 

```
At hour 11:
historical vector = (historical average at hour 1, historical average at hour 2, .., historical average at hour 11)  
current vector = (current requests at hour 1, current requests at hour 2, .., current requests at hour 11)  
```

The canberra distance (https://en.wikipedia.org/wiki/Canberra_distance) calculates the difference between the historical and current vector, identifying the top five articles with the highest distance and saves them to Redis.

Also at the end of day, another Spark job is triggered by cron to calculate a cummulative moving average from the most recent pageveiw data. That data is then used to update the historical average.

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

3. Run the producer (You can run 2 or 3 producers depending on throughput you require, you could also provide a real life stream if you have it) in the 'Stream' folder:

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

4. Run the anomaly detection job every hour when new hourly data has arrived on S3 from Flink, you could use cron or preferably Luigi (Luigi, or Apache Airflow is preferable because it can be triggered whenever data appears on S3)

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

