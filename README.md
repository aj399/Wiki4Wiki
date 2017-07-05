# Wiki4Wiki

## Table of Contents
1. [Summary](README.md## summary)
2. [PipeLine](README.md## pipeline)
3. [Dependencies](README.md## dependency)
4. [Installation](README.md## installation)
5. [Slides](README.md## slides)
6. [WebSite](README.md## website)

## Summary
  My application discovers anomalies in page requests of Wikipedia articles , by juxtaposing 7 months of historical data with the current streaming data. Every minute it detects the articles that are trending in the last 1 hour among 6 million possible articles.  Historical spark batch job that averages hourly page requests of 10 million different articles over 7 months which is about 1 TB. In streaming side flink aggregates the page requests of 6 million different articles over a 1 hr period and that data is send to s3. And every hour a spark job identifies the articles with anomalous page requests pattern by juxtaposing the aggregated streaming data and the historical average

## Pipeline
![alt text](https://github.com/aj399/Wiki4Wiki/blob/master/pipeline.PNG "PipeLine")

## Dependency

This program requires:

1. Python version 2.7
2. Java version 1.8
3. Apache Flink version 1.2.1
4. Apache ZooKeeper version 3.4.9
5. Apache Kafka version 0.9.0.1
6. Redis version 3.2.6
7. PyKafka
8. Tornado Server 

## Installation
Clone the repository using git clone https://github.com/aj399/Wiki4Wiki.git

Install all the above mentioned dependencies 

### Stream Job

Create the streamingConfig.json inside the Streaming folder
Assign the following values:
"WORKERS_IP": 'flink worker ips':9092,
"MASTER_IP" : 'flink master ip':2181,
"CONSUMER_TOPIC" : 'topic name to which python producer publishes the article request',
"PRODUCER_TOPIC" : 'base topic name of the diffenet queues(queue0 - queue9) flink produces,
"REDIS_IP" : 'Redis ip address'

Run the producer( You can run 2 or 3 producers depending on throughput you require, you could also provide a real life stream if you have it)
python /Streaming/Producer pykafka_producer.py

Clean The maven repository
mvn clean package(called inside kafkaFlink folder inside Streaming folder)
Build the repository
mvn install(called inside kafkaFlink folder inside Streaming folder)
Run the flink job
/usr/local/flink/bin/flink run -c consumer.MainStream target/consumer-0.0.jar

### Batch Job

Create the batchConfig.json inside the Batch folder
Assign the following values:
"APP_NAME": 'Name of the spark job',
"S3_ACCESS_KEY": 'Acces key of your s3 bucket',
"S3_SECRET_KEY": 'Secret key of your s3 bucket',
"BUCKET_NAME": 'Name of your s3 bucket',
"OP_FOLDER_NAME": 'Permanent Folder were hourly averages of articles are stored',
"IP_FOLDER_NAME": 'Permanent folder where historical data of hourly log data is stored',
"WORK_FOLDER_NAME": 'Temporary folder where daily hourly log data is stored',
"TEMP_FOLDER_NAME": 'Temporary folder where you store your temporary results',
"REDIS_IP": "Redis ip address"

Calculate the historical averages(Call it only once on your entire historical data)
$SPARK_HOME/bin/spark-submit --master spark://'ip address of your spark master':7077 batchAvgArtView.py

Run the anomaly detection job every hour when new hourly data has arrived on s3 from flink, you could use cron or preferrably luigi(using cron now which is triggered every hour, which is not working as flink is not writing to s3 in the expected time limit, hoping to to trigger it using luigi in future)

### Web

Create the webConfig.json inside the Web folder
Assign the followin values
"REDIS_IP": ' Redis ip address',
"TRENDING": 'key of the trending topics'

Run the web end
sudo -E python tornadoapp.py

## Slides
https://docs.google.com/presentation/d/1r9yIkb7sroe-EO4-UrqISO3w14GQFEmGTvkLwsW2OXs/edit#slide=id.p

## WebSite
http://wiki4wiki.site
