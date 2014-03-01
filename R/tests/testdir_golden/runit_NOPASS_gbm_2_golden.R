setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.gbmMSEbinomial.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing smtrees data...") 
cuseH2O<- h2o.uploadFile(H2Oserver, locate("../../smalldata/cuseexpanded.csv"), key="cuseH2O")
cuseR<- read.csv(locate("../../smalldata/cuseexpanded.csv"))

Log.info("Test H2O generation of MSE for GBM")
fith2o<- h2o.gbm(x=c("Age", "Ed", "Wantsmore"), y="UsingBinom", n.trees=10, interaction.depth=2, distribution="multinomial", n.minobsinnode=2, shrinkage=.1, data=cuseH2O)

#Reported MSE from H2O through R
err<- as.data.frame(fith2o@model$err)
REPMSE<- err[11,]

#MSE Calculated by hand From H2O predicted values
pred<- as.data.frame(h2o.predict(fith2o, newdata=cuseH2O))
diff<- pred-cuseR[,16]
diff<- diff[-1,]
diffsq<- diff^2
EXPMSE<- mean(diffsq)


Log.info("Print model MSE... \n")
Log.info(paste("H2O Reported MSE  : ", REPMSE, "\t\t", "R Expected MSE   : ", EXPMSE))


Log.info("Compare model statistics in R to model statistics in H2O")
expect_equal(REPMSE, EXPMSE, tolerance = 0.10)

testEnd()
}

doTest("GBM Test: Golden GBM - MSE for Multinomial Regression (factors)", test.gbmMSEbinomial.golden)







