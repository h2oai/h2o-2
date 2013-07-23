# R -f test_R_C_prostate.R --args Path/To/H2O.R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
  stop("Usage: R -f test_R_C_prostate.R --args Path/To/H2O.R H2OServer:Port")
source(args[1])
argsplit = strsplit(args[2], ":")[[1]]
h2o = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

prostate.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
summary(prostate.hex)

myY = "1"
for(maxx in 2:8) {
  myX = 1:maxx
  myX = myX[which(myX != myY)]
  myX = paste(myX, collapse=",")
  cat("\n\nX:", myX, "\nY:", myY, "\n")
  
  prostate.glm = h2o.glm(y = myY, x = myX, data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
  print(prostate.glm)
}