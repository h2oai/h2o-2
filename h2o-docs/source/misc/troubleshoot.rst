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

.. image:: Logsdownload.png
   :width: 70%

**Step 2** 

  From your email account contact h2ostream@googlegroups.com with a
  brief description of the error you encountered, and your contact
  information. Attach the downloaded H\ :sub:`2`\ O logs downloaded from log view
  to the email before sending. 

**Step 3**  

  We will route your email to the correct engineer or data scientist
  and work to resolve your issue. 
  
Command Line Options
"""""""""""""""""""""
Users running H\ :sub:`2`\ O through terminal using java commands can specify the amount of memory to be allocated to H\ :sub:`2`\ O processes. Memory specification using java option -Xmx allocates memory 

Troubleshooting R
""""""""""""""""""

**Dependencies in R**
  
  H\ :sub:`2`\ O wrapper and H\ :sub:`2`\ O both utilize other R packages (like lattice, and
  curl). From time to time R will fail to download from CRAN and give
  an error. In that case it's best to get the binary from CRAN
  directly and install the package manually using the call:

:: 

  >install.packages("path/to/fpc/binary/file", repos = NULL, type = "binary")


  Users may find this page on installing dependencies helpful:
  http://stat.ethz.ch/R-manual/R-devel/library/utils/html/install.packages.html

**R and H\ :sub:`2`\ O** 

  In order for H\ :sub:`2`\ O and R to work together, an instance of H\ :sub:`2`\ O must be
  running, and that instance of H\ :sub:`2`\ O must be specified in the R
  workspace. If the H\ :sub:`2`\ O instance is terminated the H\ :sub:`2`\ O package in R
  will no longer work because R will no longer be able to send
  information to H\ :sub:`2`\ O's distributed analysis, and will no longer be able
  to get information back. Even if a new instance of H\ :sub:`2`\ O is started
  with the exact same IP and port number, users will need to rerun the
  initializer package h2oWrapper, and will have to restart their H\ :sub:`2`\ O
  work session. 


**Updating the Wrapper Package**

  H\ :sub:`2`\ O's R packages are headed for CRAN, but aren't there yet, and
  until recently, they were still "in development." We've made some
  great improvements to H\ :sub:`2`\ O in R. If you are having issues
  running either package, please visit http://0xdata.com/h2O/, and
  download our latest release. Follow the instructions in our R user
  documentation to install h2oWrapper (even if it is already
  installed), in order to ensure that you have the most recent
  version. If your issue persists, please let us know. 

  
 
