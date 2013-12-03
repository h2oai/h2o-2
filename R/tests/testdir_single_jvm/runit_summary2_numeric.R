source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================")


#import multimodal data set; parse as FV
test.summary2.numeric <- function(conn) {
  Log.info("Importing wonkysummary.csv data...")
  wonkysummary.hex <- h2o.importFile(conn, locate("./smalldata/wonkysummary.csv", schema = "local"), "wonky.hex")
  

#check that summary2 gives expected output
  Log.info("Check that summary gives output...")
  summary(wonkysummary.hex)
  summary_ <- summary(wonkysummary.hex)
  Log.info("Check that we get a table back from the summary(hex)")
  expect_that(summary_, is_a("table"))


#check produced values against known values
  Log.info("Check that the summary from H2O matches known good values: ")
  H2Osum<- summary(wonkysummary.hex)
  wonky.df<- read.csv(locate("../../../smalldata/wonkysummary.csv"))
  wonky.Rsum<-as.table(summary(wonky.df))
  
  Log.info("R's summary:")

  print(summary(wonky.df))
 
  Log.info("H2O's Summary:")
  print(summary_)
  
  expect_that(H2Osum, equals(wonky.Rsum))
 
  Log.info("End of test.")
  PASSS <<- TRUE
}

PASSS <- FALSE
conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("summaryTests",test.summary2.numeric(conn)), warning = function(w) WARN(w), error = function(e) FAIL(e))
if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
