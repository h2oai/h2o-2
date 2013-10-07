source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.summary.numeric <- function(con) {
  cat("\nImporting USArrests.csv data...\n")
  arrests.hex = h2o.uploadFile(H2Ocon, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  cat("\nCheck that summary works...")
  summary(arrests.hex)
  summary_ <- summary(arrests.hex)
  cat("\nCheck that we get a table back from the summary(hex)")
  expect_that(summary_, is_a("table"))
  summary_2 <- summary(tail(USArrests))
  
  cat("\nCheck that the summary of the tail of the dataset is the same as what R produces: ")
  cat("\nsummary(tail(USArrests))\n")
  
  print(summary_2)
  cat("\nsummary(tail(arrests.hex))\n")
  print(summary(tail(arrests.hex)))
  expect_that(summary(tail(arrests.hex)), equals(summary_2))

  print("End of test.")
}

test_that("summaryTests",test.summary.numeric(H2Ocon))
