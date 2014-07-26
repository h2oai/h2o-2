#!/usr/bin/python

import os
import argparse

PARSER = argparse.ArgumentParser(description="Generate tests for SpeeDRF")
PARSER.add_argument("-t", type=str, default="../data/train", help="training data prefix")

args = PARSER.parse_args()




# h2o.SpeeDRF(
# balance.classes=  depth=            nbins=            oobee=            stat.type=        y=
# classification=   importance=       nfolds=           sample.rate=      validation=
# data=             mtry=             ntree=            seed=             x=

config = {}

config["data"] = ['normal', 'missingCols', 'missingResponse', 'missingColsAndResponse']
config["options"] = []
config["depths"] = ['20']
config["nbins"] = ['1024']
config["oobee"] = ['TRUE', 'FALSE']
config["stat.type"] = ['GINI', 'ENTROPY']
config["importance"] = ['TRUE', 'FALSE']
config["nfolds"] = ['0','3']
config["sample.rate"] = ['0.67', '1.00']
config["mtry"] = ['-1','10','500000000']
config["validation"] = ['ref.hex']

# for all possible configurations
# 
for depth in config["depths"]:
	for nbin in config["nbins"]:
		for isOobee in config["oobee"]:
			for statType in config["stat.type"]:
				for isImportant in config["importance"]:
					for nfold in config["nfolds"]:
						for sampleRate in config["sample.rate"]:
							for mtry in config["mtry"]:
								for validation in config["validation"]:
									testFileName = "runit_speedrf_d{0}_nb{1}_isOob{2}_stat{3}_isIm{4}_nfold{5}_sr{6}_mtry{7}_val{8}.R".format(
										depth,nbin,isOobee,statType,isImportant,nfold,sampleRate,mtry,validation)
									testFile = open("./generated/gen."+testFileName,'w')
									# testFile.write('setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))\n')
									# testFile.write("source('../../findNSourceUtils.R')\n\n")
									x = "c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance')"							
									y = "IsDepDelayed"
									fileString = '''
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../../findNSourceUtils.R')

test.speedrf.airlines.gini<- function(conn) {{
  air.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/allyears2k_headers.zip"), "air.hex")
  #air.train.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.train.hex")
  #air.test.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.test.hex")

  #air.rf <- h2o.SpeeDRF(y= '{0}' , x = {1} , data = air.train.hex , ntree = {2}, stat.type="{3}", depth = {4}, oobee = {5}, importance = {6}, nfolds = {7}, sample.rate = {8}, mtry = {9})
  air.rf <- h2o.SpeeDRF(y= '{0}' , x = {1} , data = air.hex , ntree = {2}, stat.type="{3}", verbose=TRUE, depth = {4}, oobee = {5}, importance = {6}, nfolds = {7}, sample.rate = {8}, mtry = {9}) 
  
  preds <- h2o.predict(air.rf,air.hex)
  print(head(preds))
  print(air.rf)
  pp <- h2o.performance(data = preds$YES, reference=air.hex$IsDepDelayed)
  print(pp)
  testEnd()
}}

doTest("speedrf test air gini", test.speedrf.airlines.gini)'''.format(y, x, "50", statType, depth, isOobee, isImportant, nfold, sampleRate, mtry)
									testFile.write(fileString)
# data specific params
# x = '1:4'
# y = '5'
# mtry = ['-1', '500', '10000', '1000000000']

#######   SAMPLE TEST   #######
#
#
# setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
# source('../../findNSourceUtils.R')

# test.speedrf.airlines.gini<- function(conn) {
#   air.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/allyears2k_headers.zip"), "air.hex")
#   air.rf <- h2o.SpeeDRF(y= 'IsDepDelayed' , x = c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance'), data = air.hex , ntree = 10 , stat.type="GINI")  
  
#   preds <- h2o.predict(air.rf,air.hex)
#   print(head(preds))
#   print(air.rf)
#   pp <- h2o.performance(data = preds$YES, reference=air.hex$IsDepDelayed)
#   print(pp)
#   testEnd()
# }

# doTest("speedrf test air gini", test.speedrf.airlines.gini)









