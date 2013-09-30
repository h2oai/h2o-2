H2O in R
------------


These instructions assume you are using R 2.13.0 or later.  

**STEP 1**

The download package can be obtained by clicking on the button **Download H2O** at `http://0xdata.com/h2o <http://0xdata.com/h2o>`_.

Unzip the downloaded h2o zip file

**STEP 2**

Start an instance of H2O. If you have questions about how to do this see the notes provided at the bottom of the page for staring from a zip file. 

*Users should be aware that in order for H2O to successfully run through R, an instance of H2O must also simultaneously be running. If the instance of H2O is stopped, the R program will no longer run, and work done will be lost.* 

**STEP 3: Console Users**

In the R console install the package by entering the following command at the prompt:

  >install.packages("<unzipped h2o directory>/R/h2oWrapper_1.0.tar.gz", repos = NULL, type = "source")
  

This returns output similar to the following:



   * installing *source* package ‘h2oWrapper’ ...
   ** R
   ** preparing package for lazy loading
   ** help
   *** installing help indices
   ** building package indices
   ** testing if installed package can be loaded
   * DONE (h2oWrapper)

**STEP 3: R Studio Users** 

R Studio users can find information on running H2O through R studio by going to 
http://docs.0xdata.com/Ruser/Rwrapper.html and looking for the section for RStudio



**STEP 4** 



  >library(h2oWrapper)

**STEP 5**

Install dependencies for the R package by calling 


  >h2oWrapper.installDepPkgs()

Which returns


  Loading required package: bitops
  Loading required package: MASS
  Loading required package: cluster
  Loading required package: mclust
  Package 'mclust' version 4.2
  Loading required package: flexmix
  Loading required package: lattice
  


**STEP 6**


  >localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)



**STEP 7** 

Here is an example of using the above object in an H2O call in R



  >irisPath = system.file("extdata", "iris.csv", package="h2o")
  
  >iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
  >summary(iris.hex)





Getting started from a zip file
-------------------------------


""""""""""""""""""
1. Download the latest release of H2O as a .zip file from `the H2O website <http://0xdata.com/h2O/>`_.

2. From your terminal change your working directory to the same directory where your .zip file is saved.

3. From your terminal, unzip the .zip file.  For example:



  unzip h2o-1.7.0.520.zip

4. At the prompt enter the following commands.  (Choose a unique name (use the -name option) for yourself if other people might be running H2O in your network.)


  cd h2o-1.7.0.520 
  java -Xmx1g -jar h2o.jar -name mystats-cloud

5. Wait a few moments and the output similar to the following will appear in your terminal window:



  03:05:45.311 main      INFO WATER: ----- H2O started -----
  03:05:45.312 main      INFO WATER: Build git branch: master
  03:05:45.312 main      INFO WATER: Build git hash: f253798433c109b19acd14cb973b45f255c59f3f
  03:05:45.312 main      INFO WATER: Build git describe: f253798
  03:05:45.312 main      INFO WATER: Build project version: 1.7.0.520
  03:05:45.313 main      INFO WATER: Built by: 'jenkins'
  03:05:45.313 main      INFO WATER: Built on: 'Thu Sep 12 00:01:52 PDT 2013'
  03:05:45.313 main      INFO WATER: Java availableProcessors: 8
  03:05:45.321 main      INFO WATER: Java heap totalMemory: 0.08 gb
  03:05:45.321 main      INFO WATER: Java heap maxMemory: 0.99 gb
  03:05:45.322 main      INFO WATER: ICE root: '/tmp/h2o-tomk'
  03:05:45.364 main      INFO WATER: Internal communication uses port: 54322
  +                                  Listening for HTTP and REST traffic on  http://192.168.1.52:54321/
  03:05:45.409 main      INFO WATER: H2O cloud name: 'mystats-cloud'
  03:05:45.409 main      INFO WATER: (v1.7.0.520) 'mystats-cloud' on /192.168.1.52:54321, discovery address /236.151.114.91:60567
  03:05:45.411 main      INFO WATER: Cloud of size 1 formed [/192.168.1.52:54321]
  03:05:45.543 main      INFO WATER: Log dir: '/tmp/h2o-tomk/h2ologs'



Useful Notes
""""""""""""   

First time users may need to download and install Java
in order to run H2O. The program is available free on the web, 
and can be quickly installed. Even though you will use Java to 
run H2O, no programming is necessary. 

In the Java command entered the term -Xmx1g was used. Xmx is the
amount of memory given to H2O.  If your data set is large,
give H2O more memory (for example, -Xmx4g gives H2O four gigabytes of
memory).  For best performance, Xmx should be 4x the size of your
data, but never more than the total amount of memory on your
computer.













