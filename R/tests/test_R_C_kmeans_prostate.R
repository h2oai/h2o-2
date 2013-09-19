# R -f test_R_C_kmeans_benign.R --args H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 1)
  stop("Usage: R -f test_R_C_kmeans_benign.R --args H2OServer:Port")

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
prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
summary(prostate.hex)

for(i in 1:2) {
  prostate.km = h2o.kmeans(data = prostate.hex, centers = 5, cols = "2")
  print(prostate.km)
}