from flask import Flask,request
from flask import render_template
import time
import json
import redis
import math
from random import randint


app = Flask(__name__)
with open('webConfig.json') as config_file:    
	config = json.load(config_file)
	
REDIS = config["REDIS_IP"]
TRENDING = config["TRENDING"]
numArticles = 0
number = 10
@app.route('/')
def index():
	
	data()
	jresponse = [{'pid': randint(1,99),'userid':1} for i in range(10)]
	container = []
	for i in range(number):
		container.append('c'+str(i))
	#container = ['a','s','d','f']
	datalist = [[ randint(0,9) for i in range(9)] for j in range(number)]
	return render_template('index.html', container = container, Num = number)
@app.route('/data')
def data():
	
	conn = redis.StrictRedis(host=REDIS, port=6379, encoding='utf-8')
	wikiTrending = conn.lrange(TRENDING, 0, number)
	numArticles = len(wikiTrending)
	wikiList = []
	pageViews = []
	timeStamps = []
	wikiArticles = []
	for i in range(numArticles):
		articleTrend = []
		articleTime = []
		wikiSplit = wikiTrending[i].split("\t")
		wikiArticles.append(wikiSplit[0])
		timeStamp = wikiSplit[1]
		times = timeStamp.split(":")
		pgViews = wikiSplit[2].split(":")
		pgViews = list(map(int, pgViews))
		pageViews.append(pgViews)
		tmStamps = []
		cMin = int(times[1])
		cHr = int(times[0])
		cSCND = times[2]
		for i in range(6):
			#print "Hour"+str(cHr)
			#print "Min"+str(cMin)
			if i!=0:
				cMin += 10
			if cMin>60:
				cMin = cMin-60
				cHr = cHr+1
			if cHr==24:
				cHr = 0
			cHR = str(cHr)
			cMIN = str(cMin)
			if cHr<10:
				cHR = "0"+cHR
			if cMin<10:
				cMIN = "0"+cMIN
			if len(cSCND)<2:
				cSCND = "0"+cSCND
			tmStamps.append(cHR+":"+cMIN+":"+cSCND)
			#print tmStamps
			
		timeStamps.append(tmStamps)		
	list1 = []
	for i in range(9):
		list2 = []
		for i in range(9):
			list2.append(randint(1,9)) 
			
		list1.append(list2)
	
	#print pageViews
	#print timeStamps
	datalist = {'datalist':pageViews,
				'timelist' : timeStamps,
				'articlelist' :wikiArticles	}
	return json.dumps(datalist) 
	

# @app.route('/data')
# def getdata():


if __name__ == '__main__':
	app.run(host = "0.0.0.0",debug = True)