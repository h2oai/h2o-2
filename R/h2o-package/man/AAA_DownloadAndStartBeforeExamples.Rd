\name{AAA_DownloadAndStartBeforeExamples}
\title{
Download H2O jar file and Start H2O cloud before examples run (for H2O developers only)
}
\description{
\code{AAA_DownloadAndStartBeforeExamples}, download H2O jar file and start H2O cloud before examples run.  This is only relevant for H2O developers during the building of the CRAN package.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
}
