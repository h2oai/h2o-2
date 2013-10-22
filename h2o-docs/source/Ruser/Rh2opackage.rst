H\ :sub:`2`\ O in R
--------------------

These instructions assume you are using R 2.14.0 or later, and are intended to guide H\ :sub:`2`\ O users who wish to interact with H\ :sub:`2`\ O through their R console. The H\ :sub:`2`\ O R package manages a companion package containing H\ :sub:`2`\ O analysis tools in order to ensure that users are using matching versions of H\ :sub:`2`\ O and H\ :sub:`2`\ O R. Additionally, it provides the means for users to connect R to their running instance of H\ :sub:`2`\ O. 

**STEP 1**

The download package can be obtained by clicking on the button Download H\ :sub:`2`\ O at `http://0xdata.com/h2o <http://0xdata.com/h2o>`_.

Unzip the downloaded H\ :sub:`2`\ O zip file. 

**STEP 2**

Start an instance of H\ :sub:`2`\ O. For help with this see :ref:`GettingStartedFromaZipFile`


Users should be aware that in order for H\ :sub:`2`\ O to successfully run through R, an instance of H\ :sub:`2`\ O must also simultaneously be running. If the instance of H\ :sub:`2`\ O is stopped, the R program will no longer run, and work done will be lost. 

**STEP 3**

Install the H\ :sub:`2`\ O package, and the H\ :sub:`2`\ O client package simultaneously by entering a command similar to the following at the prompt:
(Depending on the version of H\ :sub:`2`\ O downloaded users may need to change "h2o_1.0.3.gz" to match the current file name to reflect a different version.)

::

  

  >install.packages("<unzipped h2o directory>/R/h2o_1.0.3.tar.gz", repos = NULL, type = "source")
  
 

This returns output similar to the following:

::

   * installing *source* package ‘h2o’ ...
   ** R
   ** preparing package for lazy loading
   ** help
   *** installing help indices
   ** building package indices
   ** testing if installed package can be loaded
   * DONE (h2o)



**STEP 4**

Install dependencies for the R package by calling 

::

  >h2o.installDepPkgs()

Which returns

:: 

  Loading required package: bitops
  Loading required package: MASS
  Loading required package: cluster
  Loading required package: mclust
  Package 'mclust' version 4.2
  Loading required package: flexmix
  Loading required package: lattice
  


**STEP 5**

::

  >localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)


Which returns the following output

:: 
  
  Successfully connected to http://localhost:54321 

The correct version H\ :sub:`2`\ O client package is already running in R, there is no need to call it independently. 

**STEP 7** 

Here is an example of using the above object in an H\ :sub:`2`\ O call in R

::

  >irisPath = system.file("extdata", "iris.csv", package="h2o")
  
  >iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
  >summary(iris.hex)


















