#!/usr/bin/python

import os
import argparse
import random
import itertools

PARSER = argparse.ArgumentParser(description="Generate tests for SpeeDRF")
PARSER.add_argument("-t", type=str, default="../data/train", help="training data prefix")

args = PARSER.parse_args()

# h2o.SpeeDRF(
# balance.classes=  depth=            nbins=            oobee=            stat.type=        y=
# classification=   importance=       nfolds=           sample.rate=      validation=
# data=             mtry=             ntree=            seed=             x=

config = {}

# config["data"] = ['normal', 'missingCols', 'missingResponse', 'missingColsAndResponse']
config["depths"] = ['20']
config["nbins"] = ['1024']
config["oobee"] = ['TRUE', 'FALSE']
config["stat.type"] = ['GINI', 'ENTROPY']
config["importance"] = ['TRUE', 'FALSE']
config["nfolds"] = ['0','3']
config["sample.rate"] = ['0.67', '1.00']
config["mtry"] = ['-1','10','500000000']

configs = list(itertools.product(config["depths"], config["nbins"], config["oobee"], config["stat.type"], config["importance"], config["nfolds"], config["sample.rate"], config["mtry"]))
for con in random.sample(configs, 5):
	testFileName = "runit_speedrf_depth{}_nb{}_isOob{}_stat{}_isIm{}_nfold{}_sr{}_mtry{}.R".format(*con)
	testFile = open("./generated/gen."+testFileName,'w')
	x = "c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance')"							
	y = "IsDepDelayed"
	fileString = '''
	setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
	source('../../../findNSourceUtils.R')

	test.speedrf.airlines.gini<- function(conn) {{
	  air.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/allyears2k_headers.zip"), "air.hex")
	  #air.train.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.train.hex")
	  #air.test.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.test.hex")

	  air.rf <- h2o.SpeeDRF(y= '{}' , x = {} , data = air.hex , ntree = {}, depth = {}, nbins={}, oobee = {}, stat.type="{}", importance = {}, nfolds = {}, sample.rate = {}, mtry = {}) 
	  
	  preds <- h2o.predict(air.rf,air.hex)
	  print(head(preds))
	  print(air.rf)
	  pp <- h2o.performance(data = preds$YES, reference=air.hex$IsDepDelayed)
	  print(pp)
	  testEnd()
	}}

	doTest("speedrf test air gini", test.speedrf.airlines.gini)'''.format(y, x, "50", *con)
	testFile.write(fileString)








