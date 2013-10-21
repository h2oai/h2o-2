.. _GettingStartedFromaZipFile: 

Getting Started from a Downloaded Zip File
-------------------------------------------
 

Quick Step-by-step
""""""""""""""""""
1. Download the latest release of H\ :sub:`2`\ O as a .zip file from H\ :sub:`2`\ O `website <http://0xdata.com/h2O/>`_.

2. From your terminal change your working directory to the same directory where your .zip file is saved.

3. From your terminal, unzip the .zip file.  For example:

::

  unzip h2o-1.7.0.520.zip

4. At the prompt enter the following commands. Choose a unique name (use the -name option) for yourself if other people might be running H\ :sub:`2`\ O in your network.

::

  cd h2o-1.7.0.520  #change working directory to the downloaded file
  java -Xmx1g -jar h2o.jar -name mystats-cloud 

5. Wait a few moments and the following output will appear in your terminal window:

::

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

5. Point your web-browser to:

::

  http://localhost:54321/ 

The user interface will appear in your browser, and now H\ :sub:`2`\ O is ready to go. 

Useful Notes
""""""""""""   

First time users may need to download and install Java
in order to run H\ :sub:`2`\ O. The program is available free on the web, 
and can be quickly installed. Even though you will use Java to 
run H\ :sub:`2`\ O, no programming is necessary. 

In the Java command entered the term -Xmx1g was used. Xmx is the
amount of memory given to H\ :sub:`2`\ O.  If your data set is large,
give H\ :sub:`2`\ O more memory (for example, -Xmx4g gives H\ :sub:`2`\ O four gigabytes of
memory).  For best performance, Xmx should be 4x the size of your
data, but never more than the total amount of memory on your
computer.
