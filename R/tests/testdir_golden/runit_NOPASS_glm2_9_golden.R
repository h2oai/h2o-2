setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2regularization.golden <- function(H2Oserver) {


#The objective is to test that the differences between the deviances for models produced in GLMnet and H2O with regularization are only different to a trivial degree holding alpha constant and letting lambda vary. This has been done by running 21 different models with alpha=0 and lambda varied between 0 and 100 in increasingly large increments. If H2O behaves as expected the differences between H2O and GLMnet should not be large in terms of deviance ratios, which have the same scale and units for both tools. 

Log.info("Importing Phbirths data...") 
phbH<- h2o.uploadFile(H2Oserver, locate("../smalldata/phbirths.raw.txt"), key="phbH")
phbR<- read.table(locate("../smalldata/phbirths.raw.txt"), header=T)



#Set up variables for glmnet
DV<- as.matrix(phbR[,5])
IV<- as.matrix(phbR[,1:4])

# run basic model for baseline
basicglmnet<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=0)
basicH2O<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=0, standardize=F)


H2OratioBasic<- 1-(basicH2O@model$deviance/basicH2O@model$null.deviance)

#H2O deviance explained = R deviance explained (glmnet): .52 - baseline model.

#0.3
ghalf1<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=.3)
hhalf1<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=.3, standardize=F)
halfr1<- 1-(hhalf1@model$deviance/hhalf1@model$null.deviance)
ghalf1$dev.ratio


#0.5
ghalf2<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=.5)
hhalf2<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=.5, standardize=F)
halfr2<- 1-(hhalf2@model$deviance/hhalf2@model$null.deviance)
ghalf2$dev.ratio


#0.7
ghalf3<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=.7)
hhalf3<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=.7, standardize=F)
halfr3<- 1-(hhalf3@model$deviance/hhalf3@model$null.deviance)
ghalf3$dev.ratio


#0.9
ghalf4<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=.9)
hhalf4<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=.9, standardize=F)
halfr4<- 1-(hhalf4@model$deviance/hhalf4@model$null.deviance)
ghalf4$dev.ratio


#1
g1<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=1)
h1<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=1, standardize=F)
h1r<- 1-(h1@model$deviance/h1@model$null.deviance)
g1$dev.ratio

#1.5
g1h<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=1.5)
h1h<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=1.5, standardize=F)
h1rh<- 1-(h1h@model$deviance/h1h@model$null.deviance)
g1h$dev.ratio

#2
g2<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=2)
h2<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=2, standardize=F)
h2r<- 1-(h2@model$deviance/h2@model$null.deviance)
g2$dev.ratio

#2.5
g2h<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=2.5)
h2h<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=2.5, standardize=F)
h2rh<- 1-(h2h@model$deviance/h2h@model$null.deviance)
g2h$dev.ratio

#3
g3<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=3)
h3<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=3, standardize=F)
h3r<- 1-(h3@model$deviance/h3@model$null.deviance)
g3$dev.ratio

#4
g4<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=4)
h4<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=4, standardize=F)
h4r<- 1-(h4@model$deviance/h2h@model$null.deviance)
g4$dev.ratio

#5
g5<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=5)
h5<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=5, standardize=F)
h5r<- 1-(h5@model$deviance/h5@model$null.deviance)
g5$dev.ratio

#6
g6<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=6)
h6<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=6, standardize=F)
h6r<- 1-(h6@model$deviance/h6@model$null.deviance)
g6$dev.ratio

#7
g7<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=7)
h7<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=7, standardize=F)
h7r<- 1-(h7@model$deviance/h7@model$null.deviance)
g7$dev.ratio

#8
g8<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=8)
h8<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=8, standardize=F)
h8r<- 1-(h8@model$deviance/h8@model$null.deviance)
g8$dev.ratio


#9
g9<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=9)
h9<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=9, standardize=F)
h9r<- 1-(h9@model$deviance/h9@model$null.deviance)
g9$dev.ratio


#10
g10<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=8)
h10<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=8, standardize=F)
h10r<- 1-(h10@model$deviance/h10@model$null.deviance)
g10$dev.ratio

#15
g15<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=15)
h15<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=15, standardize=F)
h15r<- 1-(h15@model$deviance/h15@model$null.deviance)
g15$dev.ratio

#20
g20<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=20)
h20<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=20, standardize=F)
h20r<- 1-(h20@model$deviance/h20@model$null.deviance)
g20$dev.ratio

#25
g25<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=25)
h25<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=25, standardize=F)
h25r<- 1-(h25@model$deviance/h25@model$null.deviance)
g25$dev.ratio

#50
g50<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=50)
h50<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=50, standardize=F)
h50r<- 1-(h50@model$deviance/h50@model$null.deviance)
g50$dev.ratio

#100
g100<- glmnet(x=IV, y=DV, family="gaussian", nlambda=1, alpha=0, lambda=100)
h100<- h2o.glm.FV(x=c("C1", "C2", "C3", "C4"), y="C5", family="gaussian", data=phbH, nfolds=0, alpha=0, lambda=100, standardize=F)
h100r<- 1-(h100@model$deviance/h100@model$null.deviance)
g100$dev.ratio

Hratios<- c(H2OratioBasic, halfr1, halfr2, halfr3, halfr4, h1r, h1rh, h2r, h2rh, h3r, h4r, h5r, h6r, h7r, h8r, h9r, h10r, h20r, h25r, h50r, h100r)

Rratios<- c(basicglmnet$dev.ratio, ghalf1$dev.ratio, ghalf2$dev.ratio, ghalf3$dev.ratio, ghalf4$dev.ratio, g1$dev.ratio, g1h$dev.ratio, g2$dev.ratio, g2h$dev.ratio, g3$h1rh, g4$dev.ratio, g5$dev.ratio, g6$dev.ratio, g7$dev.ratio, g8$dev.ratio, g9$dev.ratio, g10$dev.ratio, g15$dev.ratio, g20$dev.ratio, g25$dev.ratio, g50$dev.ratio, g100$dev.ratio)

Diff<- Rratios-Hratios
Log.info(paste("Differences between R and H2O deviance ratios  : ", Diff))

expect_equal(Hratios, Rratios, tolerance = 0.1)


   testEnd()
}

doTest("GLM Test: Regularization: Alpha=0", test.glm2regularization.golden)

