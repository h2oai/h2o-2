setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.speedrf.airlines.gini<- function(conn) {
  air.hex <- h2o.uploadFile(conn, locate( "smalldata/airlines/allyears2k_headers.zip"), "air.hex")
  air.rf <- h2o.SpeeDRF(y= 'IsDepDelayed' , x = c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance'), data = air.hex , ntree = 10 , stat.type="GINI")  
  
  preds <- h2o.predict(air.rf,air.hex)
  print(head(preds))
  print(air.rf)
  pp <- h2o.performance(data = preds$YES, reference=air.hex$IsDepDelayed)
  print(pp)
  testEnd()
}

doTest("speedrf test air gini", test.speedrf.airlines.gini)

