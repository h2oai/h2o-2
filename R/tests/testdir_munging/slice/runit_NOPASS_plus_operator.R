##
# Testing splicing on data with UUID column using '+' expression
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')


test <- function(conn) {
  print("Reading small airline data to train and test into H2O")
  airline.test.hex = h2o.uploadFile(conn, locate("smalldata/airlines/AirlinesTest.csv.zip"), key="airline.test.hex", header=TRUE)
  summary(airline.test.hex)
  print("Reading UUIDs into H2O")
  uuid.hex = h2o.uploadFile(conn, locate("smalldata/airlines/airlineUUID.csv"), key="uuid.hex", header=TRUE)
  head(uuid.hex)
  print("Check dimensions for airline matches UUIDs")
  assertCondition((nrow(airlines.test.hex)==(nrow(uuid.hex))))
  
  print("Splice UUID to both predictions :: '+' operator")
  air.uuid <- h2o.assign((airline.test.hex + uuid.hex), key="air.uuid")
  head(air.uuid)
  
  testEnd()
}

doTest("Test splicing UUID column using '+' expression", test)

