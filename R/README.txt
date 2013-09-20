Using H2O through R (Easy Start from Download)
----------------------------------------------

Prequisites
"""""""""""
0) Command line comfortability (terminal or cmd) 
        (http://www.davidbaumgold.com/tutorials/command-line/)
1) R (version 2.13 or greater) is installed
2) An H2O jar exists on the desktop of the local machine 
        (http://0xdata.com/h2o/)

From the R Console
""""""""""""""""""

These instructions assume you are using a recent version of R, and are familiar 
with the basics of using command line. 

The download package can be obtained by clicking on the button 
**Download H2O Jar** at 

  http://0xdata.com/h2o/

Once the download completes, move the downloaded file to the desktop. 

*Users should be aware that in order for H2O to successfully run through R, an 
instance of H2O must also simultaneously be running. If the instance of H2O is 
stopped, the R program will no longer run, and work done will be lost.* These 
instructions first address running H2O, and then running H2O through R. 

Open up a terminal/cmd and at the prompt enter:

  $cd Desktop/h2o-.../ #change directory to the h2o file. 
        *Note that "h2o" should be completed with the full h2o file name for 
        the file you downloaded. The file number may   change to indicate a 
        more recent version of H2O. 
  $ java -Xmx3g -jar h2o.jar -name mystats-cloud   # starts an instance of H2O. 

This returns output like the following:

  08:26:03.603 main     INFO WATER: ----- H2O started -----
  08:26:03.607 main     INFO WATER: Build git branch: (no branch)
  08:26:03.607 main     INFO WATER: Build git hash:   d78c92f3b8a4c765b2276...
  08:26:03.607 main     INFO WATER: Build git describe: d78c92f
  08:26:03.607 main     INFO WATER: Build project version: 1.5.6.137
  08:26:03.608 main     INFO WATER: Built by: 'jenkins'
  08:26:03.608 main     INFO WATER: Built on: 'Mon Jul 29 17:10:45 PDT 2013'
  08:26:03.608 main     INFO WATER: Java availableProcessors: 2
  08:26:03.613 main     INFO WATER: Java heap totalMemory: 0.02 gb
  08:26:03.613 main     INFO WATER: Java heap maxMemory: 2.90 gb
  08:26:03.635 main     INFO WATER: ICE root: '/tmp'
  08:26:03.690 main     INFO WATER: Internal communication uses port: 54322
  +         Listening for HTTP and REST traffic on  http://  192.168.1.94:54321/
  08:26:03.775 main      INFO WATER: H2O cloud name: 'mystats-cloud'
  08:26:03.775 main      INFO WATER: (v1.5.6.137) 'mystats-cloud' on 
                  /192.168.1.94:54321,   discovery address /236.151.114.91:60567
  08:26:03.779 main     INFO WATER: Cloud of size 1 formed [/192.168.1.94:54321]
  08:26:03.779 main     INFO WATER: Log dir: '/tmp/h2ologs'

**Minimize** the terminal window, and open R. 

In the R console, install the H2O wrapper library by entering the following command 
at the prompt:

  >install.packages("/Users/UserName/Desktop/h2o_file_name/R/filename.tar.gz", 
                    repos = NULL, type = "source")

**To find the tar.gz file name**, go to h2o file -> R, and find the file with 
the extension ".tar.gz."  

For example, a user at 0x data enters the following into her R console at the 
command prompt:

  >install.packages("/Users/Irene/Desktop/h2o-1.7.0.1034/R/h2oWrapper_1.7.0.1034.tar.gz", 
                    repos = NULL, type = "source")

Which returns the following output:

  * installing *source* package ‘h2oWrapper’ ...
  ** R
  ** preparing package for lazy loading
  ** help
  *** installing help indices
  ** building package indices
  ** testing if installed package can be loaded
  * DONE (h2oWrapper)
 

**R Studio users** can install the H2O wrapper package by finding the tabbed menu 
"File; Plots; Packages; Help" and choosing *Packages*. 
Clicking on *Install Packages* brings up an installation helper. 
Choose *Package Archive File (tgz; .tar.gz)* in the *"Install From"* field. 
Click browse and follow the helper to specify Desktop -> h2o file -> R -> 
                                  .tar.gz  *Click "Open". Click "Install"*


All R users (both console and R Studio) enter the command: 

  > require(h2oWrapper)

The H2O wrapper library connects to an instance of H2O and installs the correct version
of the H2O R package that allows you to run H2O algorithms from within R.

In the R terminal enter:

  > h2oWrapper.installDepPkgs()

This installs RCurl, rjson and other package dependencies from CRAN. Then, enter:

  > h2oWrapper.init()

Which returns the following output:

  Successfully connected to http://127.0.0.1:54321
  Downloading and installing H2O R package version 1.7.0.1034
  
  * installing *source* package ‘h2o’ ...
  ** R
  ** demo
  ** inst
  ** preparing package for lazy loading
  Creating a generic function for ‘mean’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘colnames’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘nrow’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘ncol’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘summary’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘as.data.frame’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘head’ from package ‘utils’ in package ‘h2o’
  Creating a generic function for ‘tail’ from package ‘utils’ in package ‘h2o’
  Creating a generic function for ‘predict’ from package ‘stats’ in package ‘h2o’
  ** help
  *** installing help indices
  ** building package indices
  ** testing if installed package can be loaded
  * DONE (h2o)

  Success
  You may now type 'library(h2o)' to load the R package

The H2O R package has been installed. In the R terminal, enter:

  > require(h2o)
  > localH2O = new("H2OClient")
  > h2o.checkClient(localH2O)

Users can now run H2O from their R console. Additional R documentation can be 
found here

  https://github.com/0xdata/h2o/blob/master/R/h2o-package/h2o_package.pdf   

Users can now run H2O from their R or R Studio console. Additional R 
documentation can be found in the R section of the main user documentation page.
Users can also enter **??h2o** at any time to access help. 

**Users can change the amount of memory allocated to H2O.** In the Java command 
entered in the terminal to start H2O the term **-Xmx2g** was used. Xmx is the 
amount of memory given to H2O. If your data set is large, give H2O more memory 
(for example, -Xmx4g gives H2O four gigabytes of memory). For best performance, 
Xmx should be 4x the size of your data, but never more than the total amount of 
memory on your computer. 
