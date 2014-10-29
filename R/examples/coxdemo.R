# The following two commands remove any previously installed H2O packages for R.
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download, install and initialize the H2O package for R.
#install.packages("h2o", repos=c("http://s3.amazonaws.com/h2o-release/h2o/master/1569/R", getOption("repos")))

library(h2o)
h2oHandle <- h2o.init(nthreads = -1)
pbc.hex <- as.h2o(h2oHandle, pbc, key = "pbc.hex")
capture.output(h2o.exec(pbc.hex$statusOf2  <- pbc.hex$status == 2))
capture.output(h2o.exec(pbc.hex$logBili    <- log(pbc.hex$bili)))
capture.output(h2o.exec(pbc.hex$logProtime <- log(pbc.hex$protime)))
capture.output(h2o.exec(pbc.hex$logAlbumin <- log(pbc.hex$albumin)))
pbcmodel <- h2o.coxph(x = c("age", "edema", "logBili", "logProtime", "logAlbumin"), y = c("time", "statusOf2"),
                      data = pbc.hex)
summary(pbcmodel)
pbcsurv <- survfit(pbcmodel)
summary(pbcsurv)
plot(pbcsurv)

