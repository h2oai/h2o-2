Using H2O through R (Easy Start from Download)
----------------------------------------------

From the R Console
""""""""""""""""""

These instructions assume you are using a recent version of R, and are familiar with the basics of using command line. 

The download package can be obtained by clicking on the button **Download H2O Jar** at 

  http://0xdata.com/h2O/

Once the download completes, move the downloaded file to the desktop. 

*Users should be aware that in order for H2O to successfully run through R, an instance of H2O must also simultaneously be running. If the instance of H2O is stopped, the R program will no longer run, and work done will be lost.* 

These instructions first address running H2O, and then running H2O through R. 

Open the command line terminal. Mac and Windows users not familiar with command line may find this article helpful:

  http://www.davidbaumgold.com/tutorials/command-line/

At the terminal prompt enter the following commands:

  $cd Desktop/h2o-.../ #change directory to the h2o file. Note that "h2oÉ" should be   completed with the full h2o file name for the file you downloaded. The file number may   change to indicate a more recent version of H2O. 
  $ java -Xmx3g -jar h2o.jar -name mystats-cloud   # starts an instance of H2O. 

This returns output like the following:


  08:26:03.603 main      INFO WATER: ----- H2O started -----
  08:26:03.607 main      INFO WATER: Build git branch: (no branch)
  08:26:03.607 main      INFO WATER: Build git hash:   d78c92f3b8a4c765b2276a40aa2422ec396b62ce
  08:26:03.607 main      INFO WATER: Build git describe: d78c92f
  08:26:03.607 main      INFO WATER: Build project version: 1.5.6.137
  08:26:03.608 main      INFO WATER: Built by: 'jenkins'
  08:26:03.608 main      INFO WATER: Built on: 'Mon Jul 29 17:10:45 PDT 2013'
  08:26:03.608 main      INFO WATER: Java availableProcessors: 2
  08:26:03.613 main      INFO WATER: Java heap totalMemory: 0.02 gb
  08:26:03.613 main      INFO WATER: Java heap maxMemory: 2.90 gb
  08:26:03.635 main      INFO WATER: ICE root: '/tmp'
  08:26:03.690 main      INFO WATER: Internal communication uses port: 54322
  +                                  Listening for HTTP and REST traffic on  http://  192.168.1.94:54321/
  08:26:03.775 main      INFO WATER: H2O cloud name: 'mystats-cloud'
  08:26:03.775 main      INFO WATER: (v1.5.6.137) 'mystats-cloud' on /192.168.1.94:54321,   discovery address /236.151.114.91:60567
  08:26:03.779 main      INFO WATER: Cloud of size 1 formed [/192.168.1.94:54321]
  08:26:03.779 main      INFO WATER: Log dir: '/tmp/h2ologs'

**Minimize** the terminal window, and open R. 

In the R console install the library by entering the following command at the prompt:



  >install.packages("/Users/UserName/Desktop/h2o file name/R/ **tar.gz file name**", repos = NULL, type = "source")
  


**To find the tar.gz file name**, go to h2o file ==> R, and find the file with the extension ".tar.gz."  


For example, a user at 0x data enters the following into her R console at the command prompt:



  >install.packages("/Users/Irene/Desktop/h2o-1.5.6137/R/h2o_1.5.6.137.tar.gz", repos = NULL, 
  type = "source")

Which returns the following output



  * installing *source* package Ôh2oÕ ...
  ** R
  ** demo
  ** inst
  ** preparing package for lazy loading
  Creating a generic function for ÔcolnamesÕ from package ÔbaseÕ in package Ôh2oÕ
  Creating a generic function for ÔnrowÕ from package ÔbaseÕ in package Ôh2oÕ
  Creating a generic function for ÔncolÕ from package ÔbaseÕ in package Ôh2oÕ
  Creating a generic function for ÔsummaryÕ from package ÔbaseÕ in package Ôh2oÕ
  Creating a generic function for Ôas.data.frameÕ from package ÔbaseÕ in package Ôh2oÕ
  ** help
  *** installing help indices
  ** building package indices
  ** testing if installed package can be loaded
  * DONE (h2o)
 

**R Studio users** can install the H2O package by finding the tabbed menu "File; Plots; Packages; Help" and choosing *Packages*. Clicking on *Install Packages* brings up an installation helper. Choose *Package Archive File (tgz; .tar.gz)* in the *"Install From"* field. Click browse and follow the helper to specify Desktop ==> h2o file ==> R ==> .tar.gz file. *Click "Open". Click "Install"*


All R users (both console and R Studio) Enter the command 



  > require(h2o)

which returns the following output



  Loading required package: h2o
  Loading required package: RCurl
  Loading required package: bitops
  Loading required package: rjson

In the R terminal enter



  > localH2O = new("H2OClient")
  > h2o.checkClient(localH2O)

Which returns the following output


  Successfully connected to http://127.0.0.1:54321 

Users can now run H2O from their R console. Additional R documentation can be found here



  https://github.com/0xdata/h2o/blob/master/R/h2o-package/h2o_package.pdf   


Users can now run H2O from their R or R Studio console. Additional R documentation can be found in the R section of the main user documentation page. Users can also enter **??h2o** at any time to access help. 


**Users can change the amount of memory allocated to H2O.** In the Java command entered in the terminal to start H2O the term **-Xmx2g** was used. Xmx is the amount of memory given to H2O. If your data set is large, give H2O more memory (for example, -Xmx4g gives H2O four gigabytes of memory). For best performance, Xmx should be 4x the size of your data, but never more than the total amount of memory on your computer. 































