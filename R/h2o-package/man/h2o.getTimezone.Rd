\name{h2o.getTimezone}
\alias{h2o.getTimezone}
\title{Retrieves the time zone H2O is set to.}
\description{
\code{h2o.getTimezone}, Retrieves the time zone H2O is set to.
}
\usage{
h2o.getTimezone(client)
}

\arguments{
  \item{client}{ An \code{\linkS4class{H2OClient}} object.}
}
\details{
Tells the user what time zone all Date features is relative to. By default H2O assumes that the Date is collected in the same time zone that H2O is running under. To change the time zone before importing a data frame or running \code{as.Date} on a column use \code{h2o.setTimezone} and to see a list of applicable time zones use \code{h2o.listTimezones}.
}
\value{
Returns the name of the time zone H2O is set to.
}
\seealso{
\code{\link{h2o.setTimezone}, \link{h2o.listTimezones}, \link{as.Date.H2OParsedData}}
}
\note{
H2O will assume the same time zone as the user launching the H2O instance.
}
\examples{
library(h2o)
localH2O = h2o.init()
# Check the Timezone listed
currentTimeZone = h2o.getTimezone(localH2O)
print(currentTimeZone)

dates = c("Fri Jan 10 00:00:00 1969",
          "Tue Jan 10 04:00:00 2068",
          "Mon Dec 30 01:00:00 2002",
          "Wed Jan 1 12:00:00 2003")
df = data.frame(dates)
hdf = as.h2o(localH2O, df, "hdf", TRUE)

# Returns Dates assuming PST
hdf$ca = as.Date(hdf$dates, "\%c")
# Returns Dates assuming EST
# h2o.listTimezones(localH2O)
h2o.setTimezone(localH2O, tz = "EST")
hdf$nyc = as.Date(hdf$dates, "\%c")
hdf
}
