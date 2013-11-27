source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================\n")

test.histogram <- function (con, path, key) {
  h2o.data <- h2o.uploadFile(conn, path, key)
  #h2o.data <- new('H2OParsedData')
  #h2o.data@h2o <- con
  #h2o.data@key <- key
  h2o.hists <- histograms(h2o.data)
  df<- as.data.frame(h2o.data)
  if (is.null(h2o.hists) || length(h2o.hists) == 0) {
    h2o.rm(con,key)
    return(0)
  }
  for (i in 1:length(h2o.hists)) {     
    col.hist <- h2o.hists[[i]]
    if (is.null(col.hist)) {
       Log.info(paste("Column ", i, " in ", path," has no histogram.\n", sep = ""))
       next
    }
    if (is.null(col.hist$domains)) {
      counts <- col.hist$counts
      breaks <- col.hist$breaks
      r.hist <- hist(df[,i], breaks=breaks, right=FALSE)
      # regularize
#      A <- h2o.counts../ sum(counts)
#      B <- r.hist$counts../ sum(r.hist$counts)
#      Binv <- 1../ B
#      Binv[is.nan(Binv)] <- 0
#      chisq <- (A - B) %*% ((A - B) * B)
#      Log.info(cat("chisq = ", chisq, "\n"))
#      expect_true(chisq < 0.0001)
      expect_true(max(counts - r.hist$counts) <= 1)
      Log.info(cat("Column ", i, " in ", path," successful.\n"))
    } else {
      Log.info(cat("Column ", i, " in ", path," is enum, skipped.\n"))
    }
  }
  h2o.rm(con, key)
}



conn <- new("H2OClient", ip=myIP, port=myPort)
print("getting files...")
bucket <- getBucket('smalldata')
print(bucket)
csv.files <- list.files(bucket, recursive=T, full.names=T, pattern='*.csv$')
print(csv.files)
exclude <- c(locate("../../../smalldata//empty.csv"),
              locate("../../../smalldata//test/test_less_than_65535_unique_names.csv"),
              locate("../../../smalldata//test/test_more_than_65535_unique_names.csv"))
print(exclude)
csvtry <- csv.files[!(csv.files %in% exclude)]
csv.files <- csvtry
Log.info(csv.files)

for ( f in csv.files ) {
  print(f)
  tryCatch("Histogram Test", test.histogram(conn, f, sub('.csv$', '.hex', f)), warning = function(w) WARN(w), error = function(e) FAIL(e))
  PASS()
}
