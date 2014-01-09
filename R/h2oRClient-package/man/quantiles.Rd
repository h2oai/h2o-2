\name{quantiles}
\alias{quantiles}
\title{ Obtain and display quantiles for H2O parsed data.}
\description{ Obtain and return quantiles for an \code{\linkS4class{H2OParsedData} object. }}
\usage{quantiles(x)}
\arguments{
  \item{x}{An \code{\linkS4class{H2OParsedData} object.}
  	}}
\details{
Note that H2O parsed data objects can be quite large, and are therefore often distributed across multiple nodes in an H2O cluster. As a result, percentiles at the 1st, 5th, 10th, 25th, 33, 50, 66, 75, 90, 95, 99th, and other values cannot be returned. This range includes the 1st quantile at the 25th percentile, median at the 50th percentile, and 3rd quantile at the 75th percentile. 
}
\value{
A matrix describing the percentiles at the 1st, 5th, 10th, 25th, 33, 50, 66, 75, 90, 95, 99th, for each column in an \code{\linkS4class{H2OParsedData} object (or the requested subset of columns).  
}}

\examples{
Request quantiles for an H2O parsed data set: 

library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", 
 + package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
quantile(prostate.hex)

Request quantiles for a subset of columns in an H2O parsed data set 

quantile(prostate.hex[,3:5])
}
