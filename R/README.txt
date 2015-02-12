H2O in R
--------

These instructions assume you are using R 2.13.0 or later.  


**STEP 1: Unzip**

If you are reading this, you probably already downloaded an h2o zip file and unzipped it.
(Note:  Obtain a zip file from the Download button at http://h2o.ai)


**STEP 2: Install the H2O R Package**

In a terminal window, type:

R CMD INSTALL h2o*.gz


**STEP 3: Start a connection to H2O**

Load the H2O package in the R environment.  Start the connection between R and H2O (with defaults at ip: localhost and port: 54321).
Look at the help for h2o.init() for additional information about how to start and connect to H2O.

  > library(h2o)
  > localH2O = h2o.init()


**STEP 4: Drive H2O from R** 

  > irisPath = system.file("extdata", "iris.csv", package="h2o")
  > iris.hex = h2o.importFile(localH2O, irisPath)
  > summary(iris.hex)



Useful Notes
""""""""""""   

First time users may need to download and install Java
in order to run H2O. H2O currently supports any Java beyond Java 6. 
The program is available free on the web, 
and can be quickly installed. Even though you will use Java to 
run H2O, no programming is necessary. 

In the Java command entered to run H2O:

java -Xmx1g -jar h2o.jar

the term -Xmx1g was used. Xmx is the
amount of memory given to H2O.  If your data set is large,
give H2O more memory (for example, -Xmx4g gives H2O four gigabytes of
memory).  For best performance, Xmx should be 4x the size of your
data, but never more than the total amount of memory on your
computer. For larger data sets, running on a server or service 
with more memory available for computing is recommended. 
