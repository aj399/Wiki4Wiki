from pyspark import SparkContext, SparkConf
import json

with open('../batchConfig.json') as config_file:    
    config = json.load(config_file)
sc = SparkContext(appName=config["APP_NAME"])
ACCKEY = config["S3_ACCESS_KEY"]
SECKEY = config["S3_SECRET_KEY"]
BUCKET = config["BUCKET_NAME"]
IPFOLDER = config["IP_FOLDER_NAME"]
OPFOLDER = config["OP_FOLDER_NAME"]
for i in range(24):
	subFolder = str(i)
	if i<10:
		subFolder = "0"+subFolder
	ipRdd = sc.textFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+IPFOLDER+"/"+subFolder+"/")
	hrlyPageViewRdd = ipRdd.map(lambda x:x.split(" ")).map(lambda x:((x[1]).lower(),long(x[2])))
	zeroTuple = (0,0)
	aggrHrlyPageViewRdd = hrlyPageViewRdd.aggregateByKey(zeroTuple, lambda x,y: (x[0] + y,    x[1] + 1),lambda x,y: (x[0] + y[0], x[1] + y[1]))
	aggrHrlyPageViewRdd.persist()
	sumPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0])
	countPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[1])
	avgHrlyPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0]/x[1])
	sumPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/sum/"+subFolder+"/")
	countPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/count/"+subFolder+"/")
	avgPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+subFolder+"/")
	
