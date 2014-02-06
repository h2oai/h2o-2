setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.Rbasicfunctions_Apply.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing Iris data...") 
hiris<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/iris/iris.csv"), key="irisH2O")


iris <- iris[, 1:3]
hiris <- hiris[, 1:3]
Rapply<- apply(iris, 1, function(x) x + 1)
Ourapply<-apply(hiris, 1, function(x) x + 1)
summary(apply(hiris, 1, function(x) x + 1))


  testEnd()
}

doTest("Test Basic Function Apply", test.Rbasicfunctions_Apply.golden)

