from pyspark import SparkContext, SparkConf
import json
import datetime
import redis
import os
from operator import add

#currHr = datetime.datetime.now().hour
currHr = 5
sCurrHr = str(currHr)
if currHr<10:
	sCurrHr = "0"+sCurrHr
	
currFileName = str(datetime.datetime.now().year)+str(datetime.datetime.now().month)+str(datetime.datetime.now().day)+str(datetime.datetime.now().hour)

with open('../batchConfig.json') as config_file:    
    config = json.load(config_file)
sc = SparkContext(appName=config["APP_NAME"])
ACCKEY = config["S3_ACCESS_KEY"]
SECKEY = config["S3_SECRET_KEY"]
BUCKET = config["BUCKET_NAME"]
IPFOLDER = config["IP_FOLDER_NAME"]
OPFOLDER = config["OP_FOLDER_NAME"]
WKFOLDER = config["WORK_FOLDER_NAME"]
TMFOLDER = config["TEMP_FOLDER_NAME"]
REDIS = config["REDIS_IP"]

conn = redis.StrictRedis(host=REDIS, port=6379, encoding='utf-8')
currHrPageViewRdd = sc.textFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/"+sCurrHr).map(lambda x:x.split(" ")).map(lambda x:((x[1]).lower(),long(x[2]))).reduceByKey(add)
histHrPageViewRdd = sc.sequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+sCurrHr)
currDiffRdd = currHrPageViewRdd.join(histHrPageViewRdd).map(lambda x:(x[0], (abs(x[1][0]-x[1][1]), x[1][0]+x[1][1])))
currDiffRdd.persist()

if currHr==0:
	currDiffRddS = currDiffRdd.map(lambda x:(x[0], str(x[1][0])+";"+str(x[1][1])))
	currDiffRddS.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/prev")
	currAvgRdd = currDiffRdd.map(lambda x:(x[0],float(x[1][0])/float(x[1][1])))
	currTop10Rdd = currAvgRdd.top(10, key=lambda x:x[1])
	for i in currTop10Rdd:
		conn.sadd(str(currHr),str(i[0])+":"+str(i[1]))
	
	moveCmd="hdfs dfs -mv s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/"+sCurrHr+" s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+TMFOLDER+"/"+sCurrHr+"/"+currFileName
	os.system(moveCmd)
	
else:
	prevHrsPageViewRdd = sc.sequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/prev").mapValues(lambda x: x.split(";")).map(lambda x:(x[0],(long(x[1][0]),long(x[1][1]))))
	cumRdd = currDiffRdd.join(prevHrsPageViewRdd)
	cumDiffRdd = cumRdd.mapValues(lambda x:(x[0][0]+x[1][0], x[0][1]+x[1][1]))
	cumDiffRdd.persist()
	cumDiffRddS = cumDiffRdd.map(lambda x:(x[0], str(x[1][0])+";"+str(x[1][0])))
	cumAvgRdd = cumDiffRdd.mapValues(lambda x: float(x[0])/float(x[1]))
	cumTop10Rdd = cumAvgRdd.top(10, key=lambda x:x[1])
	for i in cumTop10Rdd:
		conn.sadd(sCurrHr,str(i[0])+":"+str(i[1]))

	removeCmd="hdfs dfs -rm -r -skipTrash s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/prev"
	os.system(removeCmd)
	cumDiffRddS.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/prev")
	moveCmd="hdfs dfs -mv s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+WKFOLDER+"/"+sCurrHr+" s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+TMFOLDER+"/"+sCurrHr+"/"+sCurrHr
	os.system(moveCmd)
