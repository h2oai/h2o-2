\name{h2o.shutdown}
\alias{h2o.shutdown}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Shutdown H2O server
}
\description{
Shuts down the specified H2O instance. All data on the server will be lost!
}
\usage{
h2o.shutdown(object, prompt = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
    An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
  }
  \item{prompt}{
    (Optional) A logical value indicating whether to prompt the user before shutting down the H2O server.
  }
}
\details{
This method checks if H2O is running at the specified IP address and port, and if it is, shuts down that H2O instance. WARNING: All data, models, and other values stored on the server will be lost! Only call this function if you and all other clients connected to the H2O server are finished and have saved your work.
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
localH2O = h2o.init(ip = "localhost", port = 54321)
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ shutdown }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
