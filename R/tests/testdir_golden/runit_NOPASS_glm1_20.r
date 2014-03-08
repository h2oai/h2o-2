setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2ridge.golden <- function(H2Oserver) {
library(penalized)

#Handmade data was built by hand to have very low correlation between predictor columns. This was done to ensure that coefficients would be relatively stable over a range of L1 penalties in ridge regression. 
Log.info("Importing Handmadedata data...") 
trainH<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/handmade.csv"))
trainR<- read.csv(locate("../smalldata/handmade.csv"), header=T)


#Set up variables for glmnet and penalized
DV<- as.matrix(trainR[,7])
IV<- as.matrix(trainR[,2:6])

#Run matching models in all three tools (GLMnet, Penalized, and H2O) for a range of lambda

p1<- penalized(response=DV, penalized=IV, lambda1=1, lambda2=0)
penalty=p1@penalty["L1"]
residsq = (p1@residuals)^2
rss= sum(residsq)
mult1 = .5*rss*(1/length(residsq))*penalty
g1<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult1, nlambda=1, standardize=F)
h1<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult1, nfolds=0, standardize=F, data=trainH)


p2<- penalized(response=DV, penalized=IV, lambda1=2, lambda2=0)
penalty=p2@penalty["L1"]
residsq = (p2@residuals)^2
rss= sum(residsq)
mult2 = .5*rss*(1/length(residsq))*penalty
g2<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult2, nlambda=1, standardize=F)
h2<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult2, nfolds=0, standardize=F, data=trainH)

p3<- penalized(response=DV, penalized=IV, lambda1=20, lambda2=0)
penalty=p3@penalty["L1"]
residsq = (p3@residuals)^2
rss= sum(residsq)
mult3 = .5*rss*(1/length(residsq))*penalty
g3<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult2, nlambda=1, standardize=F)
h3<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult3, nfolds=0, standardize=F, data=trainH)



p5<- penalized(response=DV, penalized=IV, lambda1=25, lambda2=0)
penalty=p5@penalty["L1"]
residsq = (p5@residuals)^2
rss= sum(residsq)
mult5 = .5*rss*(1/length(residsq))*penalty
g5<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult5 , nlambda=1, standardize=F)
h5<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult5, nfolds=0, standardize=F, data=trainH)


p6<- penalized(response=DV, penalized=IV, lambda1=35, lambda2=0)
penalty=p6@penalty["L1"]
residsq = (p6@residuals)^2
rss= sum(residsq)
mult6 = .5*rss*(1/length(residsq))*penalty
g6<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult6 , nlambda=1, standardize=F)
h6<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult6, nfolds=0, standardize=F, data=trainH)

p7<- penalized(response=DV, penalized=IV, lambda1=50, lambda2=0)
penalty=p7@penalty["L1"]
residsq = (p7@residuals)^2
rss= sum(residsq)
mult7 = .5*rss*(1/length(residsq))*penalty
g7<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult7 , nlambda=1, standardize=F)
h7<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult7, nfolds=0, standardize=F, data=trainH)

p8<- penalized(response=DV, penalized=IV, lambda1=80, lambda2=0)
penalty=p8@penalty["L1"]
residsq = (p8@residuals)^2
rss= sum(residsq)
mult8 = .5*rss*(1/length(residsq))*penalty
g8<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult8 , nlambda=1, standardize=F)
h8<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult8, nfolds=0, standardize=F, data=trainH)

p9<- penalized(response=DV, penalized=IV, lambda1=100, lambda2=0)
penalty=p9@penalty["L1"]
residsq = (p9@residuals)^2
rss= sum(residsq)
mult9 = .5*rss*(1/length(residsq))*penalty
g9<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult9 , nlambda=1, standardize=F)
h9<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult9, nfolds=0, standardize=F, data=trainH)


p10<- penalized(response=DV, penalized=IV, lambda1=120, lambda2=0)
penalty=p10@penalty["L1"]
residsq = (p10@residuals)^2
rss= sum(residsq)
mult10 = .5*rss*(1/length(residsq))*penalty
g10<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult10 , nlambda=1, standardize=F)
h10<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult10, nfolds=0, standardize=F, data=trainH)


p11<- penalized(response=DV, penalized=IV, lambda1=150, lambda2=0)
penalty=p11@penalty["L1"]
residsq = (p11@residuals)^2
rss= sum(residsq)
mult11 = .5*rss*(1/length(residsq))*penalty
g11<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult11 , nlambda=1, standardize=F)
h11<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult11, nfolds=0, standardize=F, data=trainH)

p12<- penalized(response=DV, penalized=IV, lambda1=180, lambda2=0)
penalty=p12@penalty["L1"]
residsq = (p12@residuals)^2
rss= sum(residsq)
mult12 = .5*rss*(1/length(residsq))*penalty
g12<- glmnet(x=IV, y=DV, family="gaussian", alpha=0, lambda= mult12 , nlambda=1, standardize=F)
h12<- h2o.glm.VA(x=c("a", "b", "c", "d", "e"), y="f", family="gaussian", alpha=0, lambda= mult12, nfolds=0, standardize=F, data=trainH)

#Get GLMnet coeffs
gc1<- coefficients(g1)
gc2<- as.matrix(coefficients(g2))
gc3<- as.matrix(coefficients(g3))
gc5<- as.matrix(coefficients(g5))
gc6<- as.matrix(coefficients(g6))
gc7<- as.matrix(coefficients(g7))
gc8<- as.matrix(coefficients(g8))
gc9<- as.matrix(coefficients(g9))
gc10<- as.matrix(coefficients(g10))
gc11<- as.matrix(coefficients(g11))
gc12<- as.matrix(coefficients(g12))

#Get Penalized coeffs
pc1<- coefficients(p1)
pc2<- as.matrix(coefficients(p2))
pc3<- as.matrix(coefficients(p3))
pc5<- as.matrix(coefficients(p5))
pc6<- as.matrix(coefficients(p6))
pc7<- as.matrix(coefficients(p7))
pc8<- as.matrix(coefficients(p8))
pc9<- as.matrix(coefficients(p9))
pc10<- as.matrix(coefficients(p10))
pc11<- as.matrix(coefficients(p11))
pc12<- as.matrix(coefficients(p12))

hc1<- h1@model$coefficients
hc2<- h2@model$coefficients
hc3<- h3@model$coefficients
hc5<- h5@model$coefficients
hc6<- h6@model$coefficients
hc7<- h7@model$coefficients
hc8<- h8@model$coefficients
hc9<- h9@model$coefficients
hc10<- h10@model$coefficients
hc11<- h11@model$coefficients
hc12<- h12@model$coefficients


#Print Coeffs from first and last models to see differences between models at extremes
# PENALIZED and GLMNET: coeffs in order of Intercept, a, b, c, d, e. H2O in order of a, b, c, d, e, Intercept
Log.info(paste("H2Ocoeffs 1 : ", hc1))
Log.info(paste("Penalized 1: ", pc1))
Log.info(paste("GLMnet 1: ", gc1))

Log.info(paste("H2Ocoeffs 12 : ", hc12))
Log.info(paste("Penalized 12: ", pc12))
Log.info(paste("GLMnet 12: ", gc12))

#Establish baseline conditions
p1<- abs(gc1[1]-pc1[1])
p2<- abs(gc2[1]-pc2[1])
p3<- abs(gc3[1]-pc3[1])
p5<- abs(gc5[1]-pc5[1])
p6<- abs(gc6[1]-pc6[1])
p7<- abs(gc7[1]-pc7[1])
p8<- abs(gc8[1]-pc8[1])
p9<- abs(gc9[1]-pc9[1])
p10<- abs(gc10[1]-pc10[1])
p11<- abs(gc11[1]-pc11[1])
p12<- abs(gc12[1]-pc12[1])

#Run baseline condition
Log.info(paste("Run Baseline Conditions: Compare Intercept ONLY between GLMnet and Penalized"))
expect_true(p1<.1)
expect_true(p2<.1)
expect_true(p3<.1)
expect_true(p5<.1)
expect_true(p6<.1)
expect_true(p7<.1)
expect_true(p8<.1)
expect_true(p9<.1)
expect_true(p10<.1)
expect_true(p11<.1)
expect_true(p12<.1)


#Test conditions
Log.info(paste("Build test statistics"))
d1<- abs(gc1[1]-h1@model$coefficients[6])
d2<- abs(gc2[1]-h2@model$coefficients[6])
d3<- abs(gc3[1]-h3@model$coefficients[6])
d5<- abs(gc5[1]-h5@model$coefficients[6])
d6<- abs(gc6[1]-h6@model$coefficients[6])
d7<- abs(gc7[1]-h7@model$coefficients[6])
d8<- abs(gc8[1]-h8@model$coefficients[6])
d9<- abs(gc9[1]-h9@model$coefficients[6])
d10<- abs(gc10[1]-h10@model$coefficients[6])
d11<- abs(gc11[1]-h11@model$coefficients[6])
d12<- abs(gc12[1]-h12@model$coefficients[6])

#Run test condition
Log.info(paste("Run Test Conditions: Compare Intercept ONLY between GLMnet and H2O"))
expect_true(d1<.1)
expect_true(d2<.1)
expect_true(d3<.1)
expect_true(d5<.1)
expect_true(d6<.1)
expect_true(d7<.1)
expect_true(d8<.1)
expect_true(d9<.1)
expect_true(d10<.1)
expect_true(d11<.1)
expect_true(d12<.1)

baseline<- t(c(p1, p2, p3, p5, p6, p7, p8, p9, p10))
diffs<- t(c(d1, d2, d3, d5, d6, d7, d8, d9, d10, d11, d12))

Log.info(paste("Baseline : ", baseline))
Log.info(paste("Differences : ", diffs))

   testEnd()
}

doTest("GLM Test: Ridge", test.glm2ridge.golden)

