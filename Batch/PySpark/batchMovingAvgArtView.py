
#Calculates Moving Averages all the historical data in the S3 folder specified by IP_FOLDER_NAME in batchConfig.json and current day data specified by TEMP_FOLDER_NAME in batchConfig.json and stores the output in the S3 folder specified by OP_FOLDER_NAME in batchConfig.json as well as moves the the current day data from TEMP_FOLDER_NAME to IP_FOLDER_NAME 

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
	TMFOLDER = config["TEMP_FOLDER_NAME"]
	for i in range(24):
		try:
			subFolder = str(i)
			if i<10:
				subFolder = "0"+subFolder  //creating folder structure of the form "00", "01"
			ipRdd = sc.textFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+TMFOLDER+"/"+subFolder+"/")
			hrlyPageViewRdd = ipRdd.map(lambda x:x.split(" ")).map(lambda x:((x[0]).lower(),long(x[2]))) # Second field is timestamp which is irrelevant as it is present in filename
			sumPageViewRdd = sc.SequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/sum/"+subFolder+"/")
			countPageViewRdd = sc.SequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/count/"+subFolder+"/")
			pAvgHrlyPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+subFolder+"/")
			cmbHrlyPageViewRdd = hrlyPageViewRdd.join(pAvgHrlyPageViewRdd).join(countPageViewRdd)
			#Cumulative moving average
			#CMAn+1 = n*CMAn  + Xn+1 equation 1
			avgHrlyPageViewRdd = cmbHrlyPageViewRdd.mapValues(lambda x:x[2]*x[1]+x[0]) #where x[0] = Xn+1, x[1] = CMAn and x[2] = n in equation 1
			countPageViewRdd = countPageViewRdd.mapValues(lambda x:x+1) #increment the count

			#used sequence files as it would be faster to read as key value pairs in the next spark job
			countPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/count/"+subFolder+"/") # Storing count so as to do moving averages, when new daily data comes in
			avgHrlyPageViewRdd.saveAsSequenceFile("s3a://"+ACCKEY+":"+SECKEY+"@"+BUCKET+"/"+OPFOLDER+"/avg/"+subFolder+"/")
		except IOError as e:
			print "I/O error({0}): {1}".format(e.errno, e.strerror)
		except:
			print "Unexpected error:", sys.exc_info()[0]
			raise
