#----------------------------------------------------------------------
# Purpose:  This test exercises building GLM/GBM/DL  model
#           for 376K rows and 6.9K columns
#----------------------------------------------------------------------

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

ipPort <- get_args(commandArgs(trailingOnly = TRUE))
myIP   <- ipPort[[1]]
myPort <- ipPort[[2]]
hdfs_name_node <- Sys.getenv(c("NAME_NODE"))
print(hdfs_name_node)

library(RCurl)
library(h2o)

running_inside_hexdata = file.exists("/mnt/0xcustomer-datasets/c28")

heading("BEGIN TEST")
conn <- h2o.init(ip=myIP, port=myPort, startH2O = FALSE)

h2o.ls(conn)
#----------------------------------------------------------------------
# Parameters for the test.
#----------------------------------------------------------------------
parse_time <- system.time(data.hex <- h2o.importFile(conn, "/mnt/0xcustomer-datasets/c28/mr_output.tsv.sorted.gz"))
print("Time it took to parse")
print(parse_time)

dim(data.hex)

s = h2o.runif(data.hex)
train = data.hex[s <= 0.8,]
valid = data.hex[s > 0.8,]

#GBM model
gbm_time <- system.time(model.gbm <- h2o.gbm(x = 3:(ncol(train)), y = 2,
  data = train, validation=valid, n.trees=10, interaction.depth=5))
print("Time it took to build DL ")
print(gbm_time)
model.gbm

pred = h2o.predict(model.gbm, valid)
perf <- h2o.performance(pred[,3], valid[,2])

PASS_BANNER()
