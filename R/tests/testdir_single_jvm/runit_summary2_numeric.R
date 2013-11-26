source('../Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")


#import multimodal data set; parse as FV
test.summary2.numeric <- function(H2Oserver) {
  Log.info("Importing wonkysummary.csv data...")
  wonkysummary.hex <- h2o.importFile(H2Oserver, "./smalldata/wonkysummary.csv")
  

#check that summary2 gives expected output  
  Log.info("Check that summary gives output...")
  summary(wonkysummary.hex) 
  summary_ <- summary(wonkysummary.hex)
  cat("Check that we get a table back from the summary(hex)")
  expect_that(summary_, is_a("table"))
  

#check produced values against known values
  cat("\nCheck that the summary from H2O matches known good values:\n ")
  H2Osum<- summary(wonkysummary.hex)
  wonky.df<- read.csv("../../../smalldata/wonkysummary.csv")
  wonky.Rsum<-as.table(summary(wonky.df))
  
  Log.info("R's summary:")

  print(summary(wonky.df))
 
  Log.info("H2O's Summary:")
  print(summary_)
  
  expect_that(H2Osum, equals(wonky.Rsum))
 
  print("End of test.")
}

H2Oserver <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("summaryTests",test.summary2.numeric(H2Oserver)), error = function(e) FAIL(e))
PASS()
