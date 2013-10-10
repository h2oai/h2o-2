\name{h2o-package}
\alias{h2o-package}
\alias{h2o}
\docType{package}
\title{
Installer for H2O R Package
}
\description{
This wrapper connects to an H2O instance and installs the corresponding R package "h2o", that allows users to run H2O via its REST API from within R. To communicate, the version of the R package must match the version of H2O, so when connecting to a new H2O cluster, it is necessary to re-run the initializer.
}
\details{
\tabular{ll}{
Package: \tab h2o\cr
Type: \tab Package\cr
Version: \tab 1.0.2\cr
Date: \tab 2013-09-13\cr
License: \tab Apache-2\cr
Depends: \tab R (>= 2.13.0), RCurl, rjson, tools\cr
}

You can install the package h2o by going here: http://0xdata.com/h2O/. Click the "Download H2O" button. Once the H2O file has downloaded call install.packages("~/h2o_1.0.2.tar.gz", repos= NULL, type = "source"). Note that you will need to provide a path to the .tar.gz file within the quotation marks in order for R to correctly find the package. Alternatively users can also change their working directory to the same file where the downloaded .tar.gz file is found, and specify just the file name.

First, you must have H2O installed and running on a computer/cluster. See the references for more details. If H2O is running on your local machine, call \code{h2o.init} with the IP and port that H2O is communicating on. By default, this is \url{http://localhost:54321}, where the IP is "localhost" and the port is "54321". If H2O is running on a cluster, you must provide the IP and port of the remote machine.
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
h2o.installDepPkgs()

# Check connection with H2O and ensure local H2O R package matches server version.
# Optionally ask for startH2O to start H2O if it's not already running.
# Note that for startH2O to work, the IP must be localhost and you must
# have installed with the Windows or Mac installer package so H2O is in 
# a known place.  startH2O requires the port to be 54321.
myIP = "localhost"
myPort = 54321
localH2O = h2o.init(ip = myIP, port = myPort, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = TRUE)

# Import iris dataset into H2O and print summary
irisPath = system.file("extdata", "iris.csv", package="h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(iris.hex)

# Attach H2O R package and run GLM demo
library(h2oRClient)
??h2o
demo(package = "h2oRClient")
demo(h2o.glm)
}
