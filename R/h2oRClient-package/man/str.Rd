\name{str}
\alias{str}
\title{
List objects and their structure
}
\description{
Obtain information about H2O parsed data objects and their structure. 
}
\usage{
str(x)
}
\arguments{
  \item{x}{
   H2O Parsed data object 
}
}
\value{
A table listing summary information including variable names, types (for example enum or number, count of observations and columns.)
}


\examples{
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
str(prostate.hex)
}

