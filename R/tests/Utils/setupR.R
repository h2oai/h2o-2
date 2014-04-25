options(echo=FALSE)
local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})

Log.info("============== Setting up R-Unit environment... ================")
Log.info("Branch: ")
system('git branch')
Log.info("Hash: ")
system('git rev-parse HEAD')

defaultPath <- locate("../../target/R/src/contrib")
ipPort <- get_args(commandArgs(trailingOnly = TRUE))
checkNLoadWrapper(ipPort)
checkNLoadPackages()


Log.info("Loading other required test packages")
if(!"glmnet" %in% rownames(installed.packages())) install.packages("glmnet")
if(!"gbm"    %in% rownames(installed.packages())) install.packages("gbm")
<<<<<<< HEAD
if(!"ROCR"   %in% rownmaes(installed.packages())) install.packages("ROCR")
=======
if(!"ROCR"   %in% rownames(installed.packages())) install.packages("ROCR")
>>>>>>> b72aab43e263693af20271efc6f6563923ec50d0
require(glmnet)
require(gbm)
require(ROCR)

#Global Variables
myIP   <- ipPort[[1]]
myPort <- ipPort[[2]]
PASSS <- FALSE
view_max <- 10000 #maximum returned by Inspect.java
SEED <- NULL
MASTER_SEED <- FALSE

