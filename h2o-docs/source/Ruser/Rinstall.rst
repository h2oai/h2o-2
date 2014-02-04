

H\ :sub:`2`\ O installation in R Console
------------------------------------------


These instructions assume you are using R  2.14.0 or later.  

**STEP 1**

The download package can be obtained by clicking on the button Download 
H\ :sub:`2`\ O at 
`http://0xdata.com/downloadtable <http://0xdata.com/downloadtable/>`_.

Unzip the downloaded H\ :sub:`2`\ O zip file.

**STEP 2**

Start an instance of H\ :sub:`2`\ O. For help with this see 
:ref:`GettingStartedFromaZipFile`

Users should be aware that in order for H\ :sub:`2`\ O to successfully
run through R, an instance of H\ :sub:`2`\ O must also simultaneously
be running. If the instance of H\ :sub:`2`\ O is stopped, the R
program will no longer run, and work done will be lost. 

**STEP 3:**

New uses can safely skip this step, while users who have previously
installed the  H\ :sub:`2`\ O R packages should uninstall them by entering the
following commands to the R console:  

::

   detach("package:h2o", unload=TRUE) 
   detach("package:h2oRClient", unload=TRUE) 
   remove.packages("h2o") 
   remove.packages("h2oRClient") 


Note: users may get warnings of the type "Error in
detatch("package:h2o", unload = TRUE): invalid 'name' argument. 
This tells users that there is no h2o package to uninstall. These
warnings can safely be ignored. 

**STEP 4:**

Install the H\ :sub:`2`\ O package, and the H\ :sub:`2`\ O client
package via the  H\ :sub:`2`\ O cran. This repository functions exactly like the R
repository, but is maintained by  H\ :sub:`2`\ O. 

::

  install.packages("h2o", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/rel-jacobi/2/R", getOption("repos"))))
 
  

**STEP 4:**

Once the  H\ :sub:`2`\ O R package has been installed, call the
package, and establish a connection to a running instance of  H\
:sub:`2`\ O. 

If there is no running instance of  H\ :sub:`2`\ O prior to using
the command "h2o.init()",  H\ :sub:`2`\ O in R will start an instance
automatically for the user. 
Note that in the call "localH2O<- h2o.init()" the h2o.init object is
being named localH2O in the R environment for use later in model
specification. Users who wish to specify a different IP, port, or heap
size can do so by entering the appropriate information within the
call. Entering the call exactly as it is written above assumes the
user wishes to connect to IP localhost and port: 54321.

::

  library(h2o)
  localH2O<- h2o.init()


Users who wish to specify a connection
with a server (rather than local host at port 54321) must explicity
state the IP address and port number in the h2o.init call. 
An example is given below, but **do not cut and paste**; users should
specify the IP and port number appropriate to their specific
environment. 

::

  library(h2o)
  localH2O = h2o.init(ip = "192.555.1.123", port = 12345, startH2O = FALSE, silentUpgrade = TRUE) 

This call *may* return the output:

**Do you want to install H2O R package 2.1.0.99999.1389130748 from the
server (Y/N)?**

Respond Y or YES. This is the mechanism by which the revision of the H\ :sub:`2`\ O R 
package and the H\ :sub:`2`\ O instance running on the server are verified as matching 
and compatible. 


**STEP 5: Upgrading Packages**

Users may wish to manually upgrade their R packages. For instance, if
you are running the bleeding edge developer build, it’s possible that
the code has changed, but that the revision number has not, in which
case manually upgrading ensures the most current version of not only
the H\ :sub:`2`\ O code, but the corresponding R code as well.

This can be done by returning to STEP 3, and following the commands
through STEP 4.


 






















