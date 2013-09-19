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
covtype.hex = h2o.importFile(serverH2O, "../../UCI/UCI-large/covtype/covtype.data")
# covtype.hex = h2o.importFile(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/covtype/covtype.20k.data")
summary(covtype.hex)

myY = "54"
myX = ""
# max_iter = 8

# L2: alpha = 0, lambda = 0
start = Sys.time()
h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0, lambda = 0)
end = Sys.time()
cat("GLM (L2) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n\n")

# Elastic: alpha = 0.5, lambda = 1e-4
start = Sys.time()
h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0.5, lambda = 1e-4)
end = Sys.time()
cat("GLM (Elastic) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n\n")

# L1: alpha = 1, lambda = 1e-4
start = Sys.time()
h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 1, lambda = 1e-4)
end = Sys.time()
cat("GLM (L1) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n\n")