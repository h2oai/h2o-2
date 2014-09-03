\name{as.Date}
\alias{as.Date}
\title{Converts a column from factor to date}
\description{ 
		
	}
\usage{as.Date(x, format = "")}

\arguments{
  \item{x}{A factor column in an object of class \code{\linkS4class{H2OParsedData}}, or data frame to be converted.}
  \item{format}{A character string.} 
}
\details{
Supports all parse tokens specified for \code{\link{strptime}}, except \code{\%u, \%U, \%w, \%W, and \%0Sn}.
Format also supports local variables that evaluate to a string.
}
\value{
Returns a column of dates stored as the number of milliseconds since the start of January 1, 1970. Negative numbers represent the number of seconds before this time, and positive numbers represent the number of seconds afterwards. \code{NA} values are preserved. 
}
\note{
Note that resulting values are in milliseconds and not the seconds stored by the \code{"\link{POSIXct}"} class.
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
df = data.frame(c("Fri Jan 10 00:00:00 1969 -0800", "Tue Jan 10 04:00:00 2068 -0800", "Mon Dec 30 01:00:00 2002 -0800", "Wed Jan 1 12:00:00 2003 -0800"))
hdf = as.h2o(localH2O, df, "hdf", TRUE)
hdf[,1] = as.Date(hdf[,1], "\%c \%z")
hdf[,1]
}
