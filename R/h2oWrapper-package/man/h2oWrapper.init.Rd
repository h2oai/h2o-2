\name{h2oWrapper.init}
\alias{h2oWrapper.init}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Connect to H2O and Install R Package
}
\description{
Connects to a H2O instance and checks the local H2O R package is the correct version.
}
\usage{
h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{ip}{
Object of class \code{"character"} representing the IP address of the H2O server.
}
  \item{port}{
Object of class \code{"numeric"} representing the port number of the H2O server.
}
  \item{startH2O}{
A logical value indicating whether to start the H2O launcher GUI if no connection with H2O is detected. This is only possible if H2O was installed from the InstallBuilder executable and \code{ip = "localhost"} or \code{ip = "127.0.0.1"}.
}
  \item{silentUpgrade}{
A logical value indicating whether to automatically install the H2O R package from the H2O server if a version mismatch is detected.
}
  \item{promptUpgrade}{
A logical value indicating whether to prompt the user to install the H2O R package from the H2O server if a version mismatch is detected. Ignored when \code{silentUpgrade = TRUE}.
}
}
\details{
This method first checks if H2O is connectable. If it cannot connect and \code{startH2O = TRUE} with IP of localhost, it will attempt to start the H2O launcher GUI, which is installed with the H2O InstallBuilder executable. Otherwise, it stops immediately with an error. Once connected, the method checks to see if the local H2O R package version matches the version of H2O running on the server. If there is a mismatch and the user indicates she wishes to upgrade, it will remove the local H2O R package and download/install the H2O R package from the server.
}
\value{
Once the package is successfully installed, this method will load it and return a \code{H2OClient} object containing the IP address and port number of the H2O server. See the \href{http://docs.0xdata.com/userguide/topR.html}{H2O R package documentation} for more details, or type \code{??h2o}.
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
# Try to create a localhost connection to H2O. Launch H2O GUI if needed.
localH2O = h2oWrapper.init()
localH2O = h2oWrapper.init(ip = "localhost", port = 54321)

# Prompt to install H2O R package from server if version mismatch
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, silentUpgrade = FALSE, promptUpgrade = TRUE)

# Automatically install H2O R package from server if version mismatch
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = FALSE, silentUpgrade = TRUE, promptUpgrade = FALSE)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
