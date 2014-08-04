\name{zzz_ShutdownAfterExamples}
\title{
Shutdown H2O cloud after examples run (for H2O developers only)
}
\description{
\code{zzz_ShutdownAfterExamples}, shutdown H2O cloud after examples run.  This is only relevant for H2O developers during the building of the CRAN package.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
h2o.shutdown(localH2O, prompt = false)
Sys.sleep(2)
}
