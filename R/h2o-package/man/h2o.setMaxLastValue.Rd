\name{h2o.setMaxLastValue}
\alias{h2o.setMaxLastValue}
\title{
Set maximum number for "Last.value".
}
\description{
  Set the maximum number that can be appended to "Last.value" frames created 
  before numbers start wrapping. By default temporary \code{\linkS4class{H2OParsedData}} 
  objects created without an assigned name will be assigned a "Last.value" name.
  To keep frames from being overwritten when "Last.value99" is created set the maximum 
  value to append to "Last.value" higher.
}
\usage{
h2o.setMaxLastValue(val = 1E9)
}
\arguments{
  \item{val}{ An integer specifying highest "Last.value" frame that can be created.}
}
\value{
  String message confirming the value has been set.
}
\examples{
  library(h2o)
  localH2O = h2o.init()
  h2o.setMaxLastValue(val = 1000)
}
