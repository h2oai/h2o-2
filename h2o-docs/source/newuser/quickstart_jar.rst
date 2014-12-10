.. _GettingStartedFromaZipFile: 

Getting Started From a Downloaded Zip File 
============================================


Quick Start Video
"""""""""""""""""

.. raw:: html

	<object width="425" height="344"><param name="movie" value="http://www.youtube.com/v/ZpTydwTWocQ&hl=en&fs=1"></param><param name="allowFullScreen" value="true"></param><embed src="http://www.youtube.com/v/ZpTydwTWocQ&hl=en&fs=1" type="application/x-shockwave-flash" allowfullscreen="true" width="425" height="344"></embed></object>
	
	
"""""""""""""""""

Important Notes
""""""""""""""""""   

Java is a pre-requisite for H2O; if you do not already have Java installed, make sure to install it before installing H2O. Java is available free on the web,
and can be installed quickly. Although Java is required to 
run H2O, no programming is necessary.
For users that only want to run H2O without compiling their own code, `Java Runtime Environment <https://www.java.com/en/download/>`_ (version 1.6 or later) is sufficient, but for users planning on compiling their own builds, we strongly recommend using `Java Development Kit 1.7 <www.oracle.com/technetwork/java/javase/downloads/>`_ or later. 

After installation, launch H2O using the argument `-Xmx`. Xmx is the
amount of memory given to H2O.  If your data set is large,
allocate more memory to H2O by using `-Xmx4g` instead of the default `-Xmx1g`, which will allocate 4g instead of the default 1g to your instance. For best performance, the amount of memory allocated to H2O should be four times the size of your data, but never more than the total amount of memory on your computer.

For more command line options, continue to read :ref:`Javahelp`.

"""""""""""""""""


Step-by-Step Walk-Through
"""""""""""""""""""""""""""
1. Download the .zip file containing the latest release of H2O from the
   H2O `downloads page <http://h2o.ai/download/>`_.

2. From your terminal, change your working directory to the same directory as the location of the .zip file.

3. From your terminal, unzip the .zip file.  For example:

::

  unzip h2o-1.7.0.520.zip

4. At the prompt, enter the following commands: 

::

  cd h2o-1.7.0.520  #change working directory to the downloaded file
  java -Xmx1g -jar h2o.jar #run the basic java command to start h2o

5. After a few moments, output similar to the following appears in your terminal window:

::

  Irenes-MacBook-Pro:target Irene$ java -Xmx3g -jar h2o.jar 
  09:54:05.400 main      INFO WATER: ----- H2O started -----
  09:54:05.402 main      INFO WATER: Build git branch: master
  09:54:05.402 main      INFO WATER: Build git hash: e44ee2cec508140fc6312e3d6874df8069eac669
  09:54:05.402 main      INFO WATER: Build git describe: nn-2-3032-ge44ee2c-dirty
  09:54:05.402 main      INFO WATER: Build project version: 2.1.0.99999
  09:54:05.402 main      INFO WATER: Built by: 'Irene'
  09:54:05.402 main      INFO WATER: Built on: 'Tue Feb  4 09:45:26 PST 2014'
  09:54:05.402 main      INFO WATER: Java availableProcessors: 8
  09:54:05.403 main      INFO WATER: Java heap totalMemory: 0.24 gb
  09:54:05.403 main      INFO WATER: Java heap maxMemory: 2.67 gb
  09:54:05.404 main      INFO WATER: Java version: Java 1.7.0_51 (from Oracle Corporation)
  09:54:05.404 main      INFO WATER: OS   version: Mac OS X 10.9.1 (x86_64)
  09:54:05.404 main      INFO WATER: ICE root: '/tmp/h2o-Irene'
  09:54:05.407 main      INFO WATER: Possible IP Address: en0 (en0), fe80:0:0:0:6203:8ff:fe91:950a%4
  09:54:05.407 main      INFO WATER: Possible IP Address: en0 (en0), 192.168.0.4
  09:54:05.408 main      INFO WATER: Possible IP Address: lo0 (lo0), fe80:0:0:0:0:0:0:1%1
  09:54:05.408 main      INFO WATER: Possible IP Address: lo0 (lo0), 0:0:0:0:0:0:0:1
  09:54:05.408 main      INFO WATER: Possible IP Address: lo0 (lo0), 127.0.0.1
  09:54:05.431 main      INFO WATER: Internal communication uses port:
  54322 Listening for HTTP and REST traffic
				   on  http: //192.168.0.4:54321/
  09:54:05.471 main      INFO WATER: H2O cloud name: 'Irene'
  09:54:05.472 main      INFO WATER: (v2.1.0.99999) 'Irene' on
                       /192.168.0.4:54321, discovery address /229.58.14.243:58682
  09:54:05.472 main      INFO WATER: If you have trouble connecting,
  try SSH tunneling from your local machine (e.g., via port 55555): 1. Open a terminal and run 'ssh -L 55555:localhost:54321 Irene@192.168.0.4'
  2. Point your browser to http://localhost:55555
  09:54:05.475 main      INFO WATER: Cloud of size 1 formed [/192.168.0.4:54321]
  09:54:05.475 main      INFO WATER: Log dir: '/tmp/h2o-Irene/h2ologs'


5. Point your web browser to:

::

  http://localhost:54321/ 

The user interface appears in your browser, and now H2O is ready to go.

.. WARNING::
  On Windows systems, Internet Explorer is frequently blocked due to
  security settings.  If you cannot reach http://localhost:54321, try using a
  different web browser, such as Firefox or Chrome.

