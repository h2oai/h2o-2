#
# na comparisons
#


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')


na_comparisons <- function(conn){
  Log.info('uploading testing dataset')
  df.h <- h2o.importFile(conn, locate('smalldata/jira/pub_213.csv'))

  Log.info('printing from h2o')
  Log.info( head(df.h) )

  df.h[, ncol(df.h)+1] <- df.h[,1] > 0
  res <- as.data.frame(table(df.h$l>0))

  loc <- as.data.frame(df.h)


  Log.info('testing table')
  expect_that( nrow(res), equals(1))
  expect_that( res[1,1], equals(1))
  expect_that( res[1,2], equals(5))

  expect_that( all(loc[,3] == c(1,1,1,NA,1,NA,1)), equals(T))


  testEnd()
}

if(F){
  # R code that does the same as above
  df <- read.csv('/Users/earl/work/h2o/smalldata/jira/pub_213.csv')
  df[,3] <- df[,1] > 0
  table(df$l > 0)
}


doTest('na_comparisons', na_comparisons)
