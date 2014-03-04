#
# ddply
#


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')



ddplytest <- function(conn){
  Log.info('uploading ddply testing dataset')
  df.h <- h2o.importFile(conn, 'smalldata/jira/pub-180.csv')

  Log.info('printing from h2o')
  Log.info( head(df.h) )

  Log.info('grouping over a single column (equivalent to tapply)')
  fn1 <- function(df){ min(df$col1)}
  h2o.addFunction(conn, fn1)
  df.h.1 <- ddply(df.h, .(colgroup), fn1)

  Log.info('grouping over multiple columns (equivalent to tapply with IDX=group1 + group2)')
  fn2 <- function(df){ min(df$col1)}
  h2o.addFunction(conn, fn2)
  df.h.2 <- ddply(df.h, .(colgroup, colgroup2), fn2)

  Log.info('single grouping column, use 2 columns')
  fn3 <- function(df){ min(df$col1 + df$col2) }
  h2o.addFunction(conn, fn3)
  df.h.3 <- ddply(df.h, .(colgroup), fn3)

  Log.info('grouping multiple columns, use 2 columns')
  fn4 <- function(df){ min(df$col1 + df$col2) }
  h2o.addFunction(conn, fn4)
  df.h.4 <- ddply(df.h, .(colgroup, colgroup2), fn4)

  Log.info('pulling data locally')
  df.1 <- as.data.frame( df.h.1 )
  df.2 <- as.data.frame( df.h.2 )
  df.3 <- as.data.frame( df.h.3 )
  df.4 <- as.data.frame( df.h.4 )
  
  Log.info('sorting (in ascending order) data by first column for comparisons')
  df.1 <- df.1[order(df.1[,1]),]
  df.2 <- df.2[order(df.2[,1]),]
  df.3 <- df.3[order(df.3[,1]),]
  df.4 <- df.4[order(df.4[,1]),]
  
  Log.info('testing')
  expect_equal( dim(df.1), c(3, 2) )
  expect_equal(df.1[,1], factor(c('a', 'b', 'c')))
  expect_equal(df.1[,2], c(1,3,5))

  expect_equal( dim(df.2), c(5, 3) )
  expect_equal(df.2[,1], factor(c('a', 'b', 'b', 'c', 'c')))
  expect_equal(df.2[,2], factor(paste('group', c(1,1,3,1,2), sep='')))
  expect_equal(df.2[,3], c(1,3,7,5,5))


  expect_equal( dim(df.3), c(3, 2) )
  expect_equal(df.3[,1], factor(c('a', 'b', 'c')))
  expect_equal(df.3[,2], c(3,7,11))


  expect_equal( dim(df.4), c(5, 3) )
  expect_equal(df.4[,1], factor(c('a', 'b', 'b', 'c', 'c')))
  expect_equal(df.4[,2], factor(paste('group', c(1,1,3,1,2), sep='')))
  expect_equal(df.4[,3], c(3,7,18,11,11))


  testEnd()
}

if(F){
  # R code that does the same as above
  library(plyr)
  data <- read.csv(locate('smalldata/jira/pub-180.csv'), header=T)

  # example 1 in plain R
  # semantically, these produce much the same thing, although one puts in a dataframe and the other in a named vector
  # sql GROUP BY colgroup
  tapply(data$col1, data$colgroup, min)
  ddply(data, .(colgroup), function(df){min(df$col1)} )

  # example 2 -- equivalent to sql GROUP BY colgroup, colgroup2;
  tapply(df$col1, paste(df$colgroup,df$colgroup2), min)
  ddply(data, .(colgroup, colgroup2), function(df){min(df$col1)} )

  # example 3 - can't build with tapply
  ddply(data, .(colgroup), function(df){ min(df$col1 + df$col2)} )

  # example 4 - can't build with tapply
  ddply(data, .(colgroup, colgroup2), function(df){ min(df$col1 + df$col2)} )
}


doTest('ddply', ddplytest)
