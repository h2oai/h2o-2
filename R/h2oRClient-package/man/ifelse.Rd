\name{ifelse}
\alias{ifelse}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Applies conditional statements to H2O parsed data objects. 
}
\description{
Applies conditional statements to numeric vectors in H2O parsed data objects.
}
\usage{
ifelse(test, yes, no)
}

\arguments{
  \item{test}{A locgial description of the condition to be met. See examples.
}
  \item{yes}{The value to return if the condition is TRUE.
}
  \item{no}{The value to return if the condition is FALSE. 
}
}
\details{
Only numeric values can be tested, and only numeric results can be returned for either condition. 
}
\value{
Retruns a vector of new values matching the conditions stated in the ifelse call. 
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
ausPath = system.file("extdata", "australia.csv", package="h2oRClient")
australia.hex = h2o.importFile(localH2O, path = ausPath)
australia.hex[,9]<- ifelse(australia.hex[,3]<279.9,1,0)
summary(australia.hex)
}

