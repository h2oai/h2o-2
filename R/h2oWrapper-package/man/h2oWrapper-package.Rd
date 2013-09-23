\name{h2oWrapper-package}
\alias{h2oWrapper-package}
\alias{h2oWrapper}
\docType{package}
\title{
Installer for H2O R Package
}
\description{
This wrapper connects to an H2O instance and installs the corresponding R package that allows users to run H2O via its REST API from within R. To communicate, the version of the R package must match the version of H2O, so when connecting to a new H2O cluster, it is necessary to re-run the initializer.
}
\details{
\tabular{ll}{
Package: \tab h2oWrapper\cr
Type: \tab Package\cr
Version: \tab 1.0\cr
Date: \tab 2013-09-13\cr
License: \tab Apache-2\cr
Depends: \tab R (>= 2.13.0), RCurl, rjson, tools\cr
}
First, you must have H2O installed and running on a computer/cluster. See the references for more details. If H2O is running on your local machine, call \code{h2oWrapper.init} with the IP and port that H2O is communicating on. By default, this is \url{http://localhost:54321}. If H2O is running on a cluster, you must provide the IP and port of the remote machine.
}
\author{
Anqi Fu

Maintainer: Anqi Fu <anqi@0xdata.com>
}
\references{
\itemize{
  \item \href{http://www.0xdata.com}{0xdata Homepage}
  \item \href{http://docs.0xdata.com}{H2O Documentation}
  \item \href{https://github.com/0xdata/h2o}{H2O on Github}
  }
}
\keyword{ package }
\examples{
# Install H2O R package dependencies
h2oWrapper.installDepPkgs()

# Check connection with H2O and ensure local H2O R package matches server version
myIP = "localhost"
myPort = 54321
localH2O = h2oWrapper.init(ip = myIP, port = myPort, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)

# Import iris dataset into H2O and print summary
irisPath = system.file("extdata", "iris.csv", package="h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(iris.hex)

# Attach H2O R package and run GLM demo
library(h2o)
??h2o
demo(package = "h2o")
demo(h2o.glm)
}
