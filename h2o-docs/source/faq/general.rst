.. _General_Issues:


General Issues
==============

## Downloading and Sending Logs

1. From the drop-down **Admin** menu, select **Inspect Log** or go to <http://localhost:54321/LogView.html>.
2. On the **Log View** page, click the **Download Logs** button in the
upper left hand corner to download your logs.

.. Image:: Logsdownload.png
   :width: 70%


3. Email the logs to <h2ostream@googlegroups.com>  or <support@0xdata.com> and include the following information: 
	- H\ :sub:`2`\ O version number
	- Your environment (laptop or server)
	- Your operating system (Linux, Windows, OS X)
	- Any other programs you are using with H<sub>2</sub>O (Hadoop, Cloudera, R)
	- If you are using a cluster or other specific configuration

	along with a brief description of the error you encountered and your contact
	information. Make sure to attach the downloaded H<sub>2</sub>O logs to the email before sending.

We will route your email to the correct engineer or data scientist
and work to resolve your issue.




Tunneling between servers with H\ :sub:`2`\ O
"""""""""""""""""""""""""""""""""""""""""""""

**Step 1**

Log in to the machine where H\ :sub:`2`\ O will run using ssh

**Step 2**

Start an instance of H\ :sub:`2`\ O by locating the working directory and
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
 +                                  Listening for HTTP and REST traffic on
 +                                  http://192.168.1.173:55599/
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

You are now using H\ :sub:`2`\O from localhost:55577, but the
instance of H\ :sub:`2`\O is running on the remote server (in this
case the server with the ip address 192.168.1.xxx) at port number 55599.

To see this in action note that the web UI is pointed at
localhost:55577, but that the cluster status shows the cluster running
on 192.168.1.173:55599


.. Image:: Clusterstattunnel.png
    :width: 70%

Common Troubleshooting Questions
""""""""""""""""""""""""""""""""

**Why is "Upload" is no longer working?**

This can occur when a user’s local disk is full or almost full. 
Free up space on your local disk, and the behavior should resolve. 

**What the 'Exclude' field on the Parse page mean?**

In the event a directory rather than a single file is imported, the user can choose certain files to drop or not parse.
All other files in the folder if not specified in the "Exclude" argument are parsed together as a single data object with the common header.

**Why is H2O not launching from the command line?**

::

   $ java -jar h2o.jar &

   % Exception in thread "main" java.lang.ExceptionInInitializerError
   at java.lang.Class.initializeClass(libgcj.so.10)
   at water.Boot.getMD5(Boot.java:73)
   at water.Boot.<init>(Boot.java:114)
   at water.Boot.<clinit>(Boot.java:57)
   at java.lang.Class.initializeClass(libgcj.so.10)
    Caused by: java.lang.IllegalArgumentException
   at java.util.regex.Pattern.compile(libgcj.so.10)
   at water.util.Utils.<clinit>(Utils.java:1286)
   at java.lang.Class.initializeClass(libgcj.so.10)
   ...4 more

The only prerequiste for running H\ :sub:`2`\ O is a compatiable version of Java. We recommend `Oracle's Java 1.7 <http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html>`_.

**I launched H2O instances on my nodes but why won't they cloud up?**

When launching without specifying the IP address by adding argument -ip:

::

  $ java -Xmx20g -jar h2o.jar -flatfile flatfile.txt -port 54321

and multiple local IP addresses are detected, H\ :sub:`2`\ O will fall back to default 127.0.0.1 as shown below:

::

  10:26:32.266 main      WARN WATER: Multiple local IPs detected:
  +                                    /198.168.1.161  /198.168.58.102
  +                                  Attempting to determine correct address...
  10:26:32.284 main      WARN WATER: Failed to determine IP, falling back to localhost.
  10:26:32.325 main      INFO WATER: Internal communication uses port: 54322
  +                                  Listening for HTTP and REST traffic
  +                                  on http://127.0.0.1:54321/
  10:26:32.378 main      WARN WATER: Flatfile configuration does not include self:
  /127.0.0.1:54321 but contains [/192.168.1.161:54321, /192.168.1.162:54321]

To avoid falling back to 127.0.0.1 on servers with multiple local IP addresses just run the command with the -ip argument forcing a launch at the appropriate location:

::

  $ java -Xmx20g -jar h2o.jar -flatfile flatfile.txt -ip 192.168.1.161 -port 54321


**Parse Error: "Parser setup appears to be broken, got SVMLight data with (estimated) 0 columns."**

H2O do not currently support a leading label line. Convert a row:

::

  i 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1
  j 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1

to

::

  1 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1
  2 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1

and the file should parse.

**H2O doesn't export a model with more than 10 trees?**

Please `contact us <support@0xdata.com>`_ for a license after which you can run H2O with -license argument and export larger models.

::

  java -Xmx1g -jar h2o.jar -license h2oeval.asc

