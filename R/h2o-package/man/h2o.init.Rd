\name{h2o.init}
\alias{h2o.init}
\title{Connect to H2O and Install R Package}
\description{
Connects to a running H2O instance and checks the local H2O R package is the correct version (i.e. that the version of the R package and the version of H2O are the same).}
\usage{
h2o.init(ip = "127.0.0.1", port = 54321, startH2O = TRUE, Xmx = "1g", beta = FALSE)}

\arguments{
  \item{ip}{Object of class \code{"character"} representing the IP address of the server where H2O is running.}
  \item{port}{Object of class \code{"numeric"} representing the port number of the H2O server.}
  \item{startH2O}{ (Optional) A logical value indicating whether to start the H2O launcher GUI if no connection with H2O is detected. This is only possible if H2O was installed from the InstallBuilder executable and \code{ip = "localhost"} or \code{ip = "127.0.0.1"}.}
  \item{Xmx}{(Optional) A string specifying the maximum size, in bytes, of the memory allocation pool to H2O. This value must a multiple of 1024 greater than 2MB. Append the letter m or M to indicate megabytes, or g or G to indicate gigabytes.}
  \item{beta}{(Optional) A logical value indicating whether H2O should be launch in beta mode.}
}
\details{
This method first checks if H2O is connectable. If it cannot connect and \code{startH2O = TRUE} with IP of localhost, it will attempt to start an instance of H2O with IP = localhost, port = 54321. Otherwise, it stops immediately with an error. Once connected, the method checks to see if the local H2O R package version matches the version of H2O running on the server. If there is a mismatch and the user indicates she wishes to upgrade, it will remove the local H2O R package and download/install the H2O R package from the server.
}
\value{
Once the package is successfully installed, this method will load it and return a \code{H2OClient} object containing the IP address and port number of the H2O server. See the \href{http://docs.0xdata.com/userguide/topR.html}{H2O R package documentation} for more details, or type \code{??h2o}.
}
\note{ Users may wish to manually upgrade their package (rather than waiting 
	until being prompted), which requires that they fully uninstall and reinstall 
	the H2O package, and the H2O client package. You must unload packages running 
	in the environment before upgrading. It's reccomended that users restart 
	R or R studio after upgrading.
}

\seealso{ 
\code{\link{h2o.shutdown}}
}

\examples{
# Try to create a localhost connection to H2O. 
localH2O = h2o.init()
localH2O = h2o.init(ip = "localhost", port = 54321)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = FALSE)
h2o.shutdown(localH2O)
}