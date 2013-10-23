source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.histogram <- function (con, path, key) {
  h2o.uploadFile(H2Ocon, path, key)
  h2o.data <- new('H2OParsedData2')
  h2o.data@h2o <- con
  h2o.data@key <- key
  h2o.hists <- histograms(h2o.data)
  df<- as.data.frame(h2o.data)
  x <- read.csv(path)
  if (is.null(h2o.hists) || length(h2o.hists) == 0) {
    h2o.rm(con,key)
    return(0)
  }
  for (i in 1:length(h2o.hists)) {     
    col.hist <- h2o.hists[[i]]
    if (is.null(col.hist)) {
       logging(cat("Column ", i, " in ", path," has no histogram.\n"))
       next
    }
    if (is.null(col.hist$domains)) {
      counts <- col.hist$counts
      breaks <- col.hist$breaks
      r.hist <- hist(df[,i], breaks=breaks, right=FALSE)
      # regularize
#      A <- h2o.counts / sum(counts)
#      B <- r.hist$counts / sum(r.hist$counts)
#      Binv <- 1 / B
#      Binv[is.nan(Binv)] <- 0
#      chisq <- (A - B) %*% ((A - B) * B)
#      logging(cat("chisq = ", chisq, "\n"))
#      expect_true(chisq < 0.0001)
      expect_true(max(counts - r.hist$counts) <= 1)
      logging(cat("Column ", i, " in ", path," successful.\n"))
    } else {
      logging(cat("Column ", i, " in ", path," is enum, skipped.\n"))
    }
  }
  h2o.rm(con, key)
}

csv.files <- list.files('../../smalldata/', recursive=T, full.names=T, pattern='*.csv$')
exclude <- c("../../smalldata//empty.csv",
              "../../smalldata//test/test_less_than_65535_unique_names.csv",
              "../../smalldata//test/test_more_than_65535_unique_names.csv")
csv.files <- csv.files[-which(csv.files %in% exclude)]
for ( f in csv.files ) {
  print(f)
  test.histogram(H2Ocon, f, sub('.csv$', '.hex', f))
}
