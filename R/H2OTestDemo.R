# Test R functionality for Demo
# to invoke this you need R 3.0.0 as of now
# R -f H2OTestDemo.R 
source("../R/H2O.R")
h2o.SERVER="localhost:54321"

# Run expressions on covtype
# h2o.importFile("covtype", paste(getwd(), "../smalldata/covtype/covtype.20k.data", sep="/"))
temp = h2o.importUrl("covtype", "http://www.stanford.edu/~anqif/covtype.20k.data")
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
cov.view <- h2o.inspect(covtype.hex)
print(cov.view$cols)
#h2o(slice(covtype.hex,100,100))
#h2o(sum(covtype.hex[12]))

# h2o.glm(covtype.hex, y = 12, case="1",family=binomial)
# h2o.glm(covtype.hex, y = 12, x = "1,2,3,4,5,6,7,8",case=1, family=binomial)
temp <- h2o.glm(covtype.hex, y = 12, case = "1", family = binomial)
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
cov.glm1 <- h2o.inspect_glm(temp$destination_key)
print(cov.glm1$coef)
print(cov.glm1$dof)
temp <- h2o.glm(covtype.hex, y = 12, x = "1,2,3,4,5,6,7,8", case = 1, family = binomial)
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
cov.glm2 <- h2o.inspect_glm(temp$destination_key)
print(cov.glm2$coef)
print(cov.glm2$dof)

#h2o.filter(covtype.hex, covtype.hex[6] < mean(covtype.hex[6]))
#h2o(covtype[1] + covtype[2] * 4 + max(covtype[6]) * covtype[1] + 7 - covtype[3])
#h2o(log(covtype[1]))
# add randomforest test
# h2o.rf(covtype.hex, class = "54", ntree = "10")
temp <- h2o.rf(covtype.hex, class = "54", ntree = "10")
while(h2o.poll(temp$response$redirect_request_args$model_key) == "") { Sys.sleep(1) }
cov.rf <- h2o.inspect_rf(temp$model_key, temp$data_key)
print(cov.rf)

# Run GLM
# h2o.importFile("prostate", paste(getwd(),"../smalldata/logreg/prostate.csv",sep="/"))
h2o.importUrl("prostate", "http://www.stanford.edu/~anqif/prostate.csv")
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
prostate.view <- h2o.inspect(prostate.hex)
print(prostate.view$cols)
# h2o.glm(prostate.hex, y = CAPSULE, x = "ID,AGE,RACE,PSA,DCAPS", family=binomial)
temp <- h2o.glm(prostate.hex, y = CAPSULE, x = "ID,AGE,RACE,PSA,DCAPS", family = binomial)
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
prostate.glm <- h2o.inspect_glm(temp$destination_key)
print(prostate.glm$coef)
print(prostate.glm$dof)

# Run K-Means
temp <- h2o.kmeans(covtype.hex, k = 10)
while(h2o.poll(temp$response$redirect_request_args$job) == "") { Sys.sleep(1) }
cov.km <- h2o.inspect_kmeans(temp$destination_key)
print(cov.km)