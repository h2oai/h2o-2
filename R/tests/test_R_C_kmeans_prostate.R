# R -f test_R_C_kmeans_benign.R --args Path/To/H2O.R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
  stop("Usage: R -f test_R_C_kmeans_benign.R --args Path/To/H2O.R H2OServer:Port")
source(args[1])
argsplit = strsplit(args[2], ":")[[1]]
h2o = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

prostate.hex = importFile(h2o, "../../smalldata/logreg/prostate.csv", "prostate.hex")
summary(prostate.hex)

for(i in 1:2) {
  prostate.km = h2o.kmeans(data = prostate.hex, centers = 5, cols = "2")
  print(prostate.km)
}