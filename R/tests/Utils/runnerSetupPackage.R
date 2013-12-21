options(echo=F)
if (!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source("h2oR.R")

ipPort <- get_args(commandArgs(trailingOnly = TRUE))
failed <<- F

removePackage <- function(package) {
    failed <<- F
    tryCatch(remove.packages(package), error = function(e) {failed <<- T})
    if (! failed) {
        print(paste("Removed package", package))
    }
}

removePackage('h2o')
removePackage('h2oRClient')

failed <<- F
tryCatch(library(h2o), error = function(e) {failed <<- T})
if (! failed) {
    stop("Failed to remove h2o library")
}

failed <<- F
tryCatch(library(h2oRClient), error = function(e) {failed <<- T})
if (! failed) {
    stop("Failed to remove h2oRClient library")
}

h2o_r_package_file <- NULL
dir_to_search = normalizePath("../../../target/R")
files = dir(dir_to_search)
for (i in 1:length(files)) {
    f = files[i]
    # print(f)
    arr = strsplit(f, '\\.')[[1]]
    # print(arr)
    lastidx = length(arr)
    suffix = arr[lastidx]
    # print(paste("SUFFIX", suffix))
    if (suffix == "gz") {
        h2o_r_package_file = f #arr[lastidx]
        break
    }
}

if (is.null(h2o_r_package_file)) {
    stop(paste("H2O package not found in", dir_to_search))
}

install.packages(paste(dir_to_search, h2o_r_package_file, sep="/"), repos = NULL, type = "source")
library(h2o)
h2o.init(ip            = ipPort[[1]], 
         port          = ipPort[[2]], 
         startH2O      = FALSE, 
         silentUpgrade = TRUE)

