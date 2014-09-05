
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.speedrf.airlines.golden<- function(conn) {
  air.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/allyears2k_headers.zip"), "air.hex")
  #air.train.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.train.hex")
  #air.test.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/AirlinesTrain.csv.zip"), "air.test.hex")

  #air.rf <- h2o.SpeeDRF(y= 'IsDepDelayed' , x = c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance') , data = air.train.hex , ntree = 50, stat.type="ENTROPY", depth = 20, oobee = FALSE, importance = FALSE, nfolds = 0, sample.rate = 0.67, mtry = -1)
  air.rf <- h2o.SpeeDRF(y= 'IsDepDelayed' , x = c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance') , data = air.hex , ntree = 50, oobee=TRUE, stat.type="ENTROPY", depth = 20, importance = FALSE, nfolds = 0, sample.rate = 0.67, mtry = -1) 
  
  preds <- h2o.predict(air.rf,air.hex)
  print(head(preds))
  print(air.rf)
  pp <- h2o.performance(data = preds$YES, reference=air.hex$IsDepDelayed)
  print(pp)
  testEnd()
}

doTest("speedrf test air golden", test.speedrf.airlines.golden)
