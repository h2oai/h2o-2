# Demo to test R functionality
# To invoke, need R 2.13.0 or higher
# R -f test_R_RF_diff_class.R --args H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 1)
	  stop("Usage: R -f test_R_RF_diff_class.R --args H2OServer:Port")

argsplit = strsplit(args[1], ":")[[1]]
myIP = argsplit[1]
myPort = as.numeric(argsplit[2])
defaultPath = "../../target/R"

# Check if H2O R wrapper package is installed
if(!"h2oWrapper" %in% rownames(installed.packages())) {
  wrapEnv = Sys.getenv("H2OWrapperDir")
  wrapName = list.files(defaultPath, pattern="h2oWrapper")[1]
  wrapPath = paste(ifelse(wrapEnv == "", defaultPath, wrapEnv), wrapName, sep="/")
  
  if(!file.exists(wrapPath))
    stop(paste("h2oWrapper package does not exist at", wrapPath))
  install.packages(wrapPath, repos = NULL, type = "source")
}

# Check that H2O R package matches version on server
library(h2oWrapper)
h2oWrapper.installDepPkgs()      # Install R package dependencies
h2oWrapper.init(ip=myIP, port=myPort, startH2O=FALSE)

# Load H2O R package and run test
library(h2o)
serverH2O = new("H2OClient", ip=myIP, port=myPort)

# Test of random forest using iris data set, different classes
iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100)
for(maxx in 0:3) {
  myIgnore = as.character(seq(0, maxx))
  iris.rf = h2o.randomForest(y = "4", x_ignore = myIgnore, data = iris.hex, ntree = 50, depth = 100)
  print(iris.rf)
}