
Preliminaries
========================

These instructions assume you have installed a recent version of R (>= 2.13.0), and are familiar with the basics of R
console commands. The H2O R package depends on the R libraries RCurl and rjson to communicate via the REST API. Thus,
before continuing, make sure you have these libraries installed. Open up R and navigate to "Packages; Install package(s)",
then select a CRAN mirror close to your physical location. In the Packages menu, select RCurl and rjson from the list and 
click "OK".

R Studio users should navigate to "Tools; Install Packages". Under the Install from: drop-down menu, select "Repository 
(CRAN, CRANextra)" and under Packages, type Rjson, rcurl. Check Install dependencies if it isn't already and hit "Install".

Running H2O
========================

Double-click on the h2o executable on your Desktop. This brings up an H2O Launcher application. The 
Browser port refers to the local port on which H2O is running, and the Java heap size is the amount of memory (in GB) that
is allocated to H2O. Depending on the amount of RAM on your computer, set this to a workable level. Once you are ready, 
hit the Start H2O button.

*** IMPORTANT *** 
Users should be aware that in order for H2O to successfully run through R, an instance of H2O must also simultaneously be 
running. If the instance of H2O is stopped, the R package methods will no longer run, and all work done will be lost!


Installing the R Package
========================

Open up R, and in the console, install the H2O R library by entering the following command at the prompt:

  install.packages("/Users/UserName/Desktop/h2o_package/ **tar.gz file name**", repos = NULL, type = "source")

Where you should replace /Users/UserName/Desktop/h2o file name/R/ **tar.gz file name** with the path to your the H2O R 
package that came with your installation. By default, the H2O installer will place the tar.gz file in the h2o_package 
folder on your Desktop.

R Studio users can install the H2O package by finding the tabbed menu “File; Plots; Packages; Help” and choosing Packages. 
Clicking on Install Packages brings up an installation helper. Choose Package Archive File (tgz; .tar.gz) in the “Install 
From” field. Click browse and follow the helper to specify Desktop ==> h2o_package ==> .tar.gz file. Click “Open”. 
Click “Install”.

If the installation was successful, you should see the following output on screen:

  * installing *source* package ‘h2o’ ...
  ** R
  ** demo
  ** inst
  ** preparing package for lazy loading
  Creating a generic function for ‘colnames’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘nrow’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘ncol’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘summary’ from package ‘base’ in package ‘h2o’
  Creating a generic function for ‘as.data.frame’ from package ‘base’ in package ‘h2o’
  ** help
  *** installing help indices
  ** building package indices
  ** testing if installed package can be loaded
  * DONE (h2o)

Running the R Demos
========================

Load the h2o package by typing library(h2o) in the console. Note: You must do this every time you start up R! 

Afterward, to access the help, type ??h2o. This should bring up a browser or Help window with clickable links to docs for 
all the H2O R methods.

To view all available demos, type demo(package = "h2o"), and to run a demo, type demo(NAME_OF_DEMO). For example, to run
the h2o.glm demo, you would enter demo(h2o.glm). To access help for a particular method, type ?NAME_OF_METHOD, for example, 
?h2o.glm. Runnable examples are given at the bottom of the documentation.

Additional information
========================

R:                    http://www.r-project.org/
R Studio:             http://www.rstudio.com/
R Manual:             http://cran.r-project.org/doc/manuals/R-intro.html
H2O Quickstart Guide: http://docs2.0xdata.com/quickstart/top.html
H2O R Package Docs:   http://docs.0xdata.com/bits/h2o_package.pdf