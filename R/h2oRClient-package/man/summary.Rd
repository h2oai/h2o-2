\name{summary}
\alias{summary}
\title{ Summarizes the columns of an H2O parsed object or data set. }
\description{
Summarizes the columns of an H2O parsed object or subset of columns and rows using vector notation (eg dataset[row, col])
}
\usage{summary(object, ...)}
\arguments{
  \item{object}{
An \code{\linkS4class{H2OParsedData} object.
}}
}
\value{
A matrix displaying the minimum, 1st quartile, median, mean, 3rd quartile and maximum for each numeric column included in the request call,a summary of the levels and member counts for each factor column.  and a the levels and member counts of the elements in factor columns for all of the columns specified in the summary call. 
}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", 
 + package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
summary(prostate.hex)
summary(prostate.hex$GLEASON)
summary(prostate.hex[,4:6])
}
