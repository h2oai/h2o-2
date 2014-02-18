\name{as.h2o}
\alias{as.h2o}

\title{
Converts an R object to an H2O object
}
\description{
Convert an R object to an H2O object, copy contents of the object to the running instance of H2O
}
\usage{
as.h2o(client, object, key = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{client}{
The h2o.init object that facilitates communication between R and H2O. 
}
  \item{object}{The object in the R environment to be converted to an H2O object.
}
  \item{key}{A reference assigned to the object in the instance of H2O (the key part of the key-value store, where the value is the R object to be converted.)
}
}
\details{
The R object to be converted to an H2O object should be named so that it can be used in subsequent analysis. Also note that the R object is converted to a parsed H2O data object, and will be treated as a data frame by H2O in subsequent analysis. 
}
\value{
Converts an R object to an H2O Parsed data object. 
}

\examples{
library(h2o)
local.h2o<- h2o.init()
summary(iris)
iris.r<- iris
iris.h2o<- as.h2o(local.h2o, iris.r, key="iris.h2o")
class(iris.h2o)
