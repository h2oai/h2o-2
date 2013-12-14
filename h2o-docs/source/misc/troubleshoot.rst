H\ :sub:`2`\ O Troubleshooting
------------------------------


**How to Send Logs:** Reporting Errors in H\ :sub:`2`\ O
---------------------------------------------------------

**Errors in Browser Based GUI**

  When you encounter an error while working in the web based GUI for
  H\ :sub:`2`\ O, following the instructions below will assist H\ :sub:`2`\ O developers in
  giving you the best possible solution. 

**Step 1** 

  Through the drop down menu **Admin** go to **Log View**. 

  On the log view page there is a **Download Logs button** in the
  upper left hand corner. Click on it to download your logs. 

.. Image:: Logsdownload.png
   :width: 70%

**Step 2** 

  From your email account contact h2ostream@googlegroups.com with a
  brief description of the error you encountered, and your contact
  information. Attach the downloaded H\ :sub:`2`\ O logs downloaded
  from log view to the email before sending. 

**Step 3**  

  We will route your email to the correct engineer or data scientist
  and work to resolve your issue. 


Common Question: Why is “Upload” is no longer working?
“”””””””””””””””””””””””””””””””””””””””””””””””””””””

This can occur when a user’s local disk is full or almost full. 
Free up space on your local disk, and the behavior should resolve. 


Troubleshooting R
""""""""""""""""""

**Dependencies in R**
  
  H\ :sub:`2`\ O and H\ :sub:`2`\ O client utilize other R packages
  (like lattice, and curl). From time to time R will fail to download
  from CRAN and give an error. In that case it's best to get the
  binary from CRAN directly and install the package manually using the call:

:: 

  >install.packages("path/to/fpc/binary/file", repos = NULL, type = "binary")


  Users may find this page on installing dependencies helpful:
  http://stat.ethz.ch/R-manual/R-devel/library/utils/html/install.packages.html

**R and H\ :sub:`2`\ O** 

  In order for H\ :sub:`2`\ O and R to work together, an instance of
  H\ :sub:`2`\ O must be running, and that instance of H\ :sub:`2`\ O must be specified in the R
  workspace. If the H\ :sub:`2`\ O instance is terminated the H\ :sub:`2`\ O package in R
  will no longer work because R will no longer be able to send
  information to H\ :sub:`2`\ O's distributed analysis, and will no
  longer be able to get information back. Even if a new instance of H\
  :sub:`2`\ O is started with the exact same IP and port number, users
  will need to rerun the initializer package h2oWrapper, and will have
  to restart their H\:sub:`2`\ O work session. 


**Updating the R Package**

  H\ :sub:`2`\ O's R packages are headed for CRAN, but aren't there yet, and
  until recently, they were still "in development." We've made some
  great improvements to H\ :sub:`2`\ O in R. If you are having issues
  running either package, please visit http://0xdata.com/downloadtable/, and
  download our latest release. Follow the instructions in our R user
  documentation to install h2oR (even if it is already
  installed), in order to ensure that you have the most recent
  version. If your issue persists, please let us know. 

**Internal Server Error in R**
  
  when passing data sets to H\ :sub:`2`\ O through R, users may see an
  error that reads “Internal Server Error.” This error tells users
  that R can’t find the file they are trying to import and parse. It
  can be corrected by specifying a path to the data file relative to
  the directory from which H2O is running (not relative to where R is
  running). Alternatively, users can change their working directory in
  R (in R studio the option is under the Session menu. In R it is
  under the menu Misc). By setting the R working directory to the same
  directory that H2O is running from, the path relative to R and
  relative to H2O will be the same. 
  



Tunneling between servers with H\ :sub:`2`\ O
---------------------------------------------

**Step 1** 

Log in to the machine where H\ :sub:`2`\ O will run using ssh

**Step 2**

Start an instance of H\ :sub:`2` \O by locating the working directory and 
calling a java command similar to the following ( the port number chosen here
is arbitrary and users might choose something different). 
::

 $ java -jar h2o.jar -port  55599

This returns output similar to the following: 

::

 irene@mr-0x3:~/target$ java -jar h2o.jar -port 55599
 04:48:58.053 main      INFO WATER: ----- H2O started -----
 04:48:58.055 main      INFO WATER: Build git branch: master
 04:48:58.055 main      INFO WATER: Build git hash: 64fe68c59ced5875ac6bac26a784ce210ef9f7a0
 04:48:58.055 main      INFO WATER: Build git describe: 64fe68c
 04:48:58.055 main      INFO WATER: Build project version: 1.7.0.99999
 04:48:58.055 main      INFO WATER: Built by: 'Irene'
 04:48:58.055 main      INFO WATER: Built on: 'Wed Sep  4 07:30:45 PDT 2013'
 04:48:58.055 main      INFO WATER: Java availableProcessors: 4
 04:48:58.059 main      INFO WATER: Java heap totalMemory: 0.47 gb
 04:48:58.059 main      INFO WATER: Java heap maxMemory: 6.96 gb
 04:48:58.060 main      INFO WATER: ICE root: '/tmp'
 04:48:58.081 main      INFO WATER: Internal communication uses port: 55600
 +                                  Listening for HTTP and REST traffic on  http://192.168.1.173:55599/
 04:48:58.109 main      INFO WATER: H2O cloud name: 'irene'
 04:48:58.109 main      INFO WATER: (v1.7.0.99999) 'irene' on
 /192.168.1.173:55599, discovery address /230 .252.255.19:59132
 04:48:58.111 main      INFO WATER: Cloud of size 1 formed [/192.168.1.173:55599]
 04:48:58.247 main      INFO WATER: Log dir: '/tmp/h2ologs'

**Step 3** 

Log into the remote machine where the running instance of H\ :sub:`2` \O will be
forwarded using a command similar to the following (where users
specified port numbers and IP address will be different)

::

  ssh -L 55577:localhost:55599 irene@192.168.1.173

**Step 4**

Check cluster status

You are now using H\ :sub:`2` \O from localhost:55577, but the
instance of H\ :sub:`2` \O is running on the remote server (in this
case the server with the ip address 192.168.1.xxx) at port number 55599. 

To see this in action note that the web UI is pointed at
localhost:55577, but that the cluster status shows the cluster running
on 192.168.1.173:55599


.. Image:: Clusterstattunnel.png
   :width: 70%










  
 
