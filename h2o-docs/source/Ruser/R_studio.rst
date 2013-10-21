

H\ :sub:`2`\ O in R Studio
---------------------------


These instructions assume you are using R Studio 2.14.0 or later.  

**STEP 1**

The download package can be obtained by clicking on the button Download H\ :sub:`2`\ O at `http://0xdata.com/h2o <http://0xdata.com/h2o>`_.

Unzip the downloaded H\ :sub:`2`\ O zip file.

**STEP 2**

Start an instance of H\ :sub:`2`\ O. For help with this see :ref:`GettingStartedFromaZipFile`


Users should be aware that in order for H\ :sub:`2`\ O to successfully run through R, an instance of H\ :sub:`2`\ O must also simultaneously be running. If the instance of H\ :sub:`2`\ O is stopped, the R program will no longer run, and work done will be lost. 


**STEP 3: R Studio Users**

Install the H\ :sub:`2`\ O package, and the H\ :sub:`2`\ O client package simultaneously by clicking on install package 

.. image:: RSinstall.png
   :width: 70%
 
Walk through the installer helper to the H\ :sub:`2`\ O downloaded folder.  

.. image:: RSfilefinder.png
   :width: 70%


Once the correct path has been specified click "Install." This will install the package in R. 


Start the H\ :sub:`2`\ O package by clicking the check box next to the package name "h2o". 
  

.. image:: RScheckbox.png
   :width: 70%




**STEP 4** 


Install dependencies for the R package by typing in the call: 

::

  >h2o.installDepPkgs()
  


**STEP 6**

Get RStudio talking to your instance of H\ :sub:`2`\ O by typing in the call: 

::

  >localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)

Your IP and port may be different, depending on whether you are running H\ :sub:`2`\ O from your computer or a server. If you are running on a server, where it says IP enter the IP address of the server, and the appropriate port number. In the picture below the IP number is everything before the colon, and the port number is the 5 digit string after the colon.


.. image:: RSipandport.png
   :width: 70%





 






















