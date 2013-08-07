# R -f test_R_B_benign.R --args Path/To/h2o-package/R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
  stop("Usage: R -f test_R_B_benign.R --args Path/To/h2o-package/R H2OServer:Port")

sourceDir <- function(path, trace = TRUE, ...) {
  for (nm in list.files(path, pattern = "\\.[RrSsQq]$")) {
    if(trace) cat(nm,":")           
    source(file.path(path, nm), ...)
    if(trace) cat("\n")
  }
}
sourceDir(args[1])
argsplit = strsplit(args[2], ":")[[1]]
localH2O = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

benign.hex = h2o.importURL(localH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
summary(benign.hex)

myY = "3"
for(maxx in 10:13) {
  myX = 0:maxx
  myX = myX[which(myX != myY)]
  myX = paste(myX, collapse=",")
  cat("\n\nX:", myX, "\nY:", myY, "\n")
  
  benign.glm = h2o.glm(y = myY, x = myX, data = benign.hex, family = "binomial", nfolds = 5, alpha = 0.5)
  print(benign.glm)
}