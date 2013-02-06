# Test R functionality for Demo
# to invoke this you need R 2.15 as of now
# R -f H2OTestDemo.R 
source("../R/H2O.R")
h2o.SERVER="localhost:54323"

# Run expressions on covtype
h2o.importFile("covtype", paste(getwd(), "../smalldata/covtype/covtype.20k.data", sep="/"))
h2o.inspect("covtype.hex")
#h2o(slice(covtype.hex,100,100))
#h2o(sum(covtype.hex[12]))
h2o.glm(covtype.hex, y = 12, case="1",family=binomial)
h2o.glm(covtype.hex, y = 12, x = "1,2,3,4,5,6,7,8",case=1, family=binomial)
#h2o.filter(covtype.hex, covtype.hex[6] < mean(covtype.hex[6]))
#h2o(covtype[1] + covtype[2] * 4 + max(covtype[6]) * covtype[1] + 7 - covtype[3])
#h2o(log(covtype[1]))
# add randomforest test
h2o.rf(covtype.hex, class = "54", ntree = "10")

# Run GLM
h2o.importFile("prostate", paste(getwd(),"../smalldata/logreg/prostate.csv",sep="/"))
h2o.inspect("prostate.hex")
h2o.glm(prostate.hex, y = CAPSULE, x = "ID,AGE,RACE,PSA,DCAPS", family=binomial)
