# R -f test_R_C_kmeans_benign.R --args Path/To/H2O_Load.R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
  stop("Usage: R -f test_R_C_kmeans_benign.R --args Path/To/H2O_Load.R H2OServer:Port")

source(args[1], chdir = TRUE)
argsplit = strsplit(args[2], ":")[[1]]
localH2O = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

covtype.hex = h2o.importFile(localH2O, "../../UCI/UCI-large/covtype/covtype.data")
# covtype.hex = h2o.importFile(localH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/covtype/covtype.20k.data")
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