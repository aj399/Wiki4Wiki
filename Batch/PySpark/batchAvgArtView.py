#Averages all the historical data in the S3 folder specified by IP_FOLDER_NAME in batchConfig.json and stores the output in the S3 folder specified by OP_FOLDER_NAME in batchConfig.json 

from pyspark import SparkContext, SparkConf
import json

try:
	with open('../batchConfig.json') as config_file:    
	    config = json.load(config_file)
except IOError:
        print 'cannot open batchConfig.json'
else:
	sc = SparkContext(appName=config["APP_NAME"])
	ACCKEY = config["S3_ACCESS_KEY"]
	SECKEY = config["S3_SECRET_KEY"]
	BUCKET = config["BUCKET_NAME"]
	IPFOLDER = config["IP_FOLDER_NAME"]
	OPFOLDER = config["OP_FOLDER_NAME"]
	for i in range(24):
		try:
			subFolder = str(i)
			if i<10:
				subFolder = "0"+subFolder  //creating folder structure of the form "00", "01"
			ipRdd = sc.textFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+IPFOLDER+"/"+subFolder+"/")
			splitRdd = ipRdd.map(lambda x:x.split(" "))
			corrRdd = splitRdd.filter(lambda x:len(x) == 4) #filter out data containing incorrect information, example no page request information
			hrlyPageViewRdd = corrRdd.map(lambda x:((x[1]).lower(),long(x[2]))) # First field is project code which is irrelevant
			zeroTuple = (0,0)
			
			# Count up the number of occurence of article(there can be a case where article comes up in same file twice, but that case is handled here), and it's total pageviews
			aggrHrlyPageViewRdd = hrlyPageViewRdd.aggregateByKey(zeroTuple, lambda x,y: (x[0] + y,    x[1] + 1),lambda x,y: (x[0] + y[0], x[1] + y[1])) 
			aggrHrlyPageViewRdd.persist()
			sumPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0])
			countPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[1])
			avgHrlyPageViewRdd = aggrHrlyPageViewRdd.mapValues(lambda x: x[0]/x[1])

			#used sequence files as it would be faster to read as key value pairs in the next spark job
			countPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/count/"+subFolder+"/") # Storing sum and count so as to do moving averages, when new daily data comes in
			avgHrlyPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+subFolder+"/")
		except IOError as e:
			print "I/O error({0}): {1}".format(e.errno, e.strerror)
		except:
			print "Unexpected error:", sys.exc_info()[0]
			raise
	
