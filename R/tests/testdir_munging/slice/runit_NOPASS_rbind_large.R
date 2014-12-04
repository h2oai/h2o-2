##
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.rbind <- function(conn) {
  Log.info('test rbind')

  hdf <- h2o.uploadFile(conn, "/home/0xdiag/datasets/standard/covtype.data")
  otherhdf <- h2o.uploadFile(conn, "/home/0xdiag/datasets/airlines/airlines_all.csv")

  Log.info("rbind self")
  print(rbind(hdf, hdf))

  Log.info("rbind otherself")
  print(rbind(otherhdf, otherhdf))


 
  Log.info("cbind then rbind")
  a <- cbind(hdf[,1], hdf[,2], hdf[,3], hdf[,4])
  
  z <- rbind(a,a,a,a)

  print(dim(z))
  print(z)

  z <- rbind( otherhdf,  otherhdf,  otherhdf,  otherhdf)
  print(dim(z))
  print(z)

  testEnd()
}

doTest("test rbind", test.rbind)

