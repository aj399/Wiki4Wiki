#Averages all the historical data in the S3 folder specified by IP_FOLDER_NAME in batchConfig.json and stores the output in the S3 folder specified by OP_FOLDER_NAME in batchConfig.json 

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
		subFolder = "0"+subFolder  //creating folder structure of the form "00", "01"
	ipRdd = sc.textFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+IPFOLDER+"/"+subFolder+"/")
	hrlyPageViewRdd = ipRdd.map(lambda x:x.split(" ")).map(lambda x:((x[1]).lower(),long(x[2]))) # First field is project code which is irrelevant
	zeroTuple = (0,0)
	aggrHrlyPageViewRdd = hrlyPageViewRdd.aggregateByKey(zeroTuple, lambda x,y: (x[0] + y,    x[1] + 1),lambda x,y: (x[0] + y[0], x[1] + y[1]))
	aggrHrlyPageViewRdd.persist()
	sumPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0])
	countPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[1])
	avgHrlyPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0]/x[1])
	
	#used sequence files as it would be faster to read as key value pairs in the next spark job
	sumPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/sum/"+subFolder+"/") # Storing sum and count so as to do moving averages, when new daily data comes in
	countPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/count/"+subFolder+"/")
	avgPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+subFolder+"/")
	
