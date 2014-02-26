H\ :sub:`2`\ O + R Console For Developers Using Git
------------------------------------------------------------------


These instructions assume you are using R  2.14.0 or later, and are a
developer who is using a local build of H\ :sub: `2`\ O from Github.
If you have downloaded an  H\ :sub: `2`\ O jar file from available downloads at 
`http://0xdata.com/downloadtable <http://0xdata.com/downloadtable/>`_
please close these instructions and visit :ref:`Rfromdownload`

**STEP 1**

Obtain  H\ :sub:`2`\ O from Git. For instructions on getting started
from Github visit :ref:`QuickstartGit`


**STEP 2**

Start an instance of H\ :sub:`2`\ O (in this example, on localhost
with 3 gigs of memory, and assuming you are in a local h2o directory).

::

  java -Xmx3g -jar target/h2o.jar

This should return output similar to the following: 

::

  11:54:30.416 main      INFO WATER: ----- H2O started -----
  11:54:30.417 main      INFO WATER: Build git branch: master
  11:54:30.417 main      INFO WATER: Build git hash: 7bc09927ee8828faf5808d93457394505384232e
  11:54:30.418 main      INFO WATER: Build git describe: nn-2-3929-g7bc0992-dirty
  11:54:30.418 main      INFO WATER: Build project version: 2.3.0.99999
  11:54:30.418 main      INFO WATER: Built by: 'Irene'
  11:54:30.418 main      INFO WATER: Built on: 'Wed Feb 26 11:22:45 PST 2014'
  11:54:30.418 main      INFO WATER: Java availableProcessors: 8
  11:54:30.419 main      INFO WATER: Java heap totalMemory: 0.24 gb
  11:54:30.419 main      INFO WATER: Java heap maxMemory: 2.67 gb
  11:54:30.419 main      INFO WATER: Java version: Java 1.7.0_51 (from Oracle Corporation)
  11:54:30.420 main      INFO WATER: OS   version: Mac OS X 10.9.2 (x86_64)
  11:54:30.420 main      INFO WATER: ICE root: '/tmp/h2o-Irene'
  11:54:30.423 main      INFO WATER: Possible IP Address: en0 (en0), fe80:0:0:0:6203:8ff:fe91:950a%4
  11:54:30.423 main      INFO WATER: Possible IP Address: en0 (en0), 192.168.1.82
  11:54:30.423 main      INFO WATER: Possible IP Address: lo0 (lo0), fe80:0:0:0:0:0:0:1%1
  11:54:30.423 main      INFO WATER: Possible IP Address: lo0 (lo0), 0:0:0:0:0:0:0:1
  11:54:30.424 main      INFO WATER: Possible IP Address: lo0 (lo0), 127.0.0.1
  11:54:30.447 main      INFO WATER: Internal communication uses port: 54322 Listening for HTTP and REST traffic on  http://192.168.1.82:54321/
  11:54:30.485 main      INFO WATER: H2O cloud name: 'Irene'
  11:54:30.486 main      INFO WATER: (v2.3.0.99999) 'Irene' on /192.168.1.82:54321, discovery address /229.58.14.243:58682
  11:54:30.486 main      INFO WATER: If you have trouble connecting,
  try SSH tunneling from your local machine (e.g., via port
  55555): 1. Open a terminal and run 'ssh -L 55555:localhost:54321
  Irene@192.168.1.82' 2. Point your browser to http://localhost:55555
  11:54:30.488 main      INFO WATER: Cloud of size 1 formed [/192.168.1.82:54321]
  11:54:30.488 main      INFO WATER: Log dir: '/tmp/h2o-Irene/h2ologs'


If users do not start an instance of H\ :sub:`2`\ O, one will be
started automatically for them at localhost: 54321 (see **STEP 5** for
more detail). 

If the instance of H\ :sub:`2`\ O is stopped, the R
program will no longer run, and work done will be lost. 

**STEP 3:** 

From your terminal at the top of your h2o directory: 

::

  open target/index.html

Which will open a browser based index page that looks similar to the
following:

.. image:: localbuildindex.png
   :width: 100 %  

**STEP 4:**

Open your preferred R interface (R console, or R Studio). 
First, uninstall all prior versions of the h2o package by running the 
following calls in your R console: 

::

   detach("package:h2o", unload=TRUE) 
   remove.packages("h2o") 


Note: users may get warnings of the type "Error in
detatch("package:h2o", unload = TRUE): invalid 'name' argument. 
This tells users that there is no  H\ :sub:`2`\ O package to uninstall. These
warnings can safely be ignored.  

**STEP 5:**

Install the H\ :sub:`2`\ O package according to the instructions shown
on the index page opened in **Step 3**. Users can cut and paste the
calls as shown on their index pages, but should not cut and paste the
calls included below as examples. 

**DO NOT CUT AND PASTE THIS CALL INTO R**
The call shown below is specifically for the build, and specific user's
file path in use when this example was written, which may not match
your build or configuration. 

  `install.packages("h2o", repos=(c("file:///Users/Tom/Work/h2o/target/R", getOption("repos"))))` 
  

**STEP 6:**

Once the  H\ :sub:`2`\ O R package has been installed, call the
package, and establish a connection to a running instance of  H\
:sub:`2`\ O. 

If there is no running instance of  H\ :sub:`2`\ O prior to using
the command "h2o.init()",  H\ :sub:`2`\ O in R will start an instance
automatically for the user at localhost:54321, and the user will be
notified.  

::

  library(h2o)
  localH2O <- h2o.init()


Users who wish to specify a connection
with a server (other than localhost at port 54321) must explicity
state the IP address and port number in the h2o.init call. 
An example is given below, but **do not cut and paste**; users should
specify the IP and port number appropriate to their specific
environment. 

::

  library(h2o)
  localH2O = h2o.init(ip = "192.555.1.123", port = 12345, startH2O = FALSE) 



 *End*
