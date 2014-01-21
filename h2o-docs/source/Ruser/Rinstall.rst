

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

Install the H\ :sub:`2`\ O package, and the H\ :sub:`2`\ O client
package simultaneously by entering the following call into your R
console with an instance of H2O already running (if H2O is not
running, an instance will be started automatically at ip=LocalHost and
port=54321).  

**DO NOT CUT AND PASTE THIS CALL.** Please replace "foo" and the
revision number  with the appropriate file path and revision number
specifying your downloaded and unzipped H2O file. 

::

  install.packages("foo/R/h2o_2.1.0.99999.tar.gz", repos = NULL, type = "source")
 
Once the correct path has been specified, the H2O package will be
installed. 

**STEP 4:**

Call:

::

  library(h2o)
  localH2O<- h2o.init()

Note that in the call "localH2O<- h2o.init()" the h2o.init object is
being named localH2O in the R environment for use later in model
specification. Users who wish to specify a different IP, port, or heap
size can do so by entering the appropriate information within the
call. Entering the call exactly as it is written above assumes the
user wishes to connect to IP localhost and port: 54321. 

This call *may* return the output:

**Do you want to install H2O R package 2.1.0.99999.1389130748 from the
server (Y/N)?**

Respond Y or YES. This is the mechanism by which the revision of the H2O R 
package and the H2O instance running on the server are verified as matching 
and compatible. 


**STEP 5: Upgrading Packages**

Users may wish to manually upgrade their R packages. For instance, if
you are running the bleeding edge developer build, it’s possible that
the code has changed, but that the revision number has not, in which
case manually upgrading ensures the most current version of not only
the H2O code, but the corresponding R code as well.

This can be done using the following commands.

**IMPORTANT**
Before you cut and paste these commands please check that the file path to 
the version ofH2O R you would like to install is correct. 

::
  
  detach("h2oRClient”)
  detach("h2oRClient”)
  remove.packages("h2oRClient")
  remove.packages("h2o")
  install.packages("foo/R/h2o_2.1.0.99999.tar.gz", repos = NULL, 
        type = "source")
  library(h2o)
  localH2O=h2o.init()

 






















