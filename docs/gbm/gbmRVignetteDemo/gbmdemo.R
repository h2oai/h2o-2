# Set the CRAN mirror
local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})

# The following two commands remove any previously installed H2O packages for R.
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download, install and initialize the H2O package for R. Do not copy and paste the installation link below. 
# Instead, go to http://0xdata.com/downloadtable/ and click on "Bleeding Edge Developer Build". The following link
# should follow the template "http://s3.amazonaws.com/h2o-release/h2o/master/****/index.html" with the 4 *'s filled
# in with digits. Replace the 4 *'s in the following command with these 4 digits before executing.

version = "****" 

### Opportunity to manually set the version (must be at least 1444)
#version = "1444"

# Fall-back: Automatically get the latest version
if (version == "****") {
    library(RCurl)
    if(url.exists("http://s3.amazonaws.com/h2o-release/h2o/master/latest")) {
        h = basicTextGatherer()
        curlPerform(url = "http://s3.amazonaws.com/h2o-release/h2o/master/latest", writefunction = h$update)
        version = h$value()
        version = gsub("\n","",version)
    }
}

install.packages("h2o", repos=(c(paste(paste("http://s3.amazonaws.com/h2o-release/h2o/master/",version,sep=""),"/R",sep=""), getOption("repos"))))

# load the library
library(h2o)

# start H2O locally
h2o_server = h2o.init()

#Check out the built-in h2o.deeplearning demo
demo(h2o.gbm)

#If not done by the user, we can unpack the data.zip file here
#unzip("data.zip")

weather.hex = h2o.uploadFile(h2o_server, path = "weather.csv", header = TRUE, sep = ",", key = "weather.hex")

#Alternatively (required for HDFS or S3 or URLs), use importFile:
#weather.hex = h2o.importFile(h2o_server, path = "<full-path-to>/weather.csv", header = TRUE, sep = ",", key = "weather.hex")

# Display a summary
summary(weather.hex)


#Load the data and prepare for modeling
air_train.hex = h2o.uploadFile(h2o_server, path = "AirlinesTrain.csv", header = TRUE, sep = ",", key = "airline_train.hex")

air_test.hex = h2o.uploadFile(h2o_server, path = "AirlinesTest.csv", header = TRUE, sep = ",", key = "airline_test.hex")

myX <- c("fDayofMonth", "fDayOfWeek")

air.model <- h2o.gbm(y = "IsDepDelayed", x = myX, 
                   distribution="multinomial", 
                   data = air_train.hex, n.trees=100, 
                   interaction.depth=4, 
                   shrinkage=0.1,
                   importance=TRUE)

#View the specified parameters of your GBM model
air.model@model$params

#Examine the performance of the trained model
air.model

#Perform classification on the held out data
prediction = h2o.predict(air.model, newdata=air_test.hex)

#Copy predictions from H2O to R
pred = as.data.frame(prediction)

head(pred)

air.model@model$varimp

air.grid <- h2o.gbm(y = "IsDepDelayed", x = myX, 
                   distribution="multinomial", 
                   data = air_train.hex, n.trees=c(5,10,15), 
                   interaction.depth=c(2,3,4), 
                   shrinkage=c(0.1,0.2))

#print out all prediction errors and run times of the models
air.grid
air.grid@model

#print out a *short* summary of each of the models (indexed by parameter)
air.grid@sumtable

#print out *full* summary of each of the models
all_params = lapply(air.grid@model, function(x) { x@model$params })
all_params

#access a particular parameter across all models
shrinkages = lapply(air.grid@model, function(x) { x@model$params$shrinkage })
