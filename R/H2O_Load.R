# Change this global variable to match your own system's path
ROOT.PATH <- "/Users/spencer/master/h2o/R/h2o-package/R/"
src <-
function() {
  warning("MAY NOT WORK ON YOUR SYSTEM -- **TRY TO CHANGE `ROOT.PATH`!**")
  to_src <- c("Wrapper.R", "Internal.R", "Classes.R", "ParseImport.R", "models.R", "Algorithms.R")
  require(rjson); require(RCurl)
  invisible(lapply(to_src,function(x){source(paste(ROOT.PATH, x, sep = ""))}))
}
src()
