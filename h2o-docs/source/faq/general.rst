.. _General_Issues:


General Issues
==============

Downloading and Sending Logs
----------------------------

***************
Accessing Logs
***************

Depending on whether you are using Hadoop with H2O and whether the job is currently running, there are different ways of obtaining the logs for H2O. 

**Note**: Unless otherwise specified, the following methods work the same for the first version of H2O (H2O1) and H2O-dev (which uses a UI known as H2O-Flow). 

Copy and email the logs to support@h2o.ai or submit them to h2ostream@googlegroups.com with a brief description of your Hadoop environment, including the Hadoop distribution and version.

********************
Without Running Jobs
********************

- If you are using Hadoop and the job is not running, view the logs by using the ``yarn logs -applicationId`` command. When you start an H2O instance, the complete command displays in the output: 

::

	amy@mr-0xb1:~/h2o-2.5.0.99999/hadoop$ hadoop jar h2odriver_hdp1.3.2.jar water.hadoop.h2odriver
	-libjars ../h2o.jar -mapperXmx 10g -nodes 4 -output output903 -verbose:class
	Determining driver host interface for mapper->driver callback...
    [Possible callback IP address: 192.168.1.161]
    [Possible callback IP address: 127.0.0.1]
	Using mapper->driver callback IP address and port: 192.168.1.161:37244
	(You can override these with -driverif and -driverport.)
	Driver program compiled with MapReduce V1 (Classic)
	Memory Settings:
	mapred.child.java.opts:      -Xms10g -Xmx10g -verbose:class
	mapred.map.child.java.opts:  -Xms10g -Xmx10g -verbose:class
	Extra memory percent:        10
	mapreduce.map.memory.mb:     11264
	Job name 'H2O_74206' submitted
	JobTracker job ID is 'job_201407040936_0030'
	For YARN users, logs command is 'yarn logs -applicationId application_201407040936_0030'
	Waiting for H2O cluster to come up...

In the above example, the command is specified in the next to last line (``For YARN users, logs command is...``). The command is unique for each instance. In Terminal, enter ``yarn logs -applicationId application_<UniqueID>`` to view the logs (where ``<UniqueID>`` is the number specified in the next to last line of the output that displayed when you created the cluster). 
	
""""""""

- Use YARN to obtain the ``stdout`` and ``stderr`` logs that are used for troubleshooting. To learn how to access YARN based on management software, version, and job status, see "Accessing YARN". 

 1. Click the **Applications** link to view all jobs, then click the **History** link for the job.
 
	.. image:: images/YARN_AllApps_History.png
	   :width: 100%

 2. Click the **logs** link. 
	
	.. image:: images/YARN_History_Logs.png
	   :width: 100%
	
 3. 	Copy the information that displays and send it in an email to support@h2o.ai. 
	
	.. image:: images/YARN_History_Logs2.png
	   :width: 100%
 
""""""""""""

******************
With Running Jobs
******************

If you are using Hadoop and the job is still running: 

- Use YARN to obtain the ``stdout`` and ``stderr`` logs that are used for troubleshooting. To learn how to access YARN based on management software, version, and job status, see "Accessing YARN".

 1. Click the **Applications** link to view all jobs, then click the **ApplicationMaster** link for the job. 
	
	.. image:: images/YARN_AllApps_AppMaster.png
	   :width: 100%

 2. Select the job from the list of active jobs. 
	
	.. image:: images/YARN_AppMaster_Job.png
	   :width: 100%
	
 3. Click the **logs** link. 
	
	 .. image:: images/YARN_AppMaster_Logs.png
	    :width: 100%
	
 4. 	Send the contents of the displayed files to support@h2o.ai. 
	
	.. image:: images/YARN_AppMaster_Logs2.png
	   :width: 100%
	
""""""""

- (H2O1) Go to the H2O web UI and select **Admin** > **Inspect Log** or go to http://localhost:54321/LogView.html. To download the logs, click the **Download Logs** button. 

 When you view the log, the output displays the location of log directory after ``Log dir:`` (as shown in the last line in the following example): 

::

	08-Jan 12:27:39.099 172.16.2.188:54321    28195 main      INFO WATER: ----- H2O started -----
	08-Jan 12:27:39.100 172.16.2.188:54321    28195 main      INFO WATER: Build git branch: rel-mirzakhani
	08-Jan 12:27:39.100 172.16.2.188:54321    28195 main      INFO WATER: Build git hash: ae31ed04e47d826b73e7180e07ba00db13e879f3
	08-Jan 12:27:39.100 172.16.2.188:54321    28195 main      INFO WATER: Build git describe: jenkins-rel-mirzakhani-2
	08-Jan 12:27:39.100 172.16.2.188:54321    28195 main      INFO WATER: Build project version: 2.8.3.2
	08-Jan 12:27:39.101 172.16.2.188:54321    28195 main      INFO WATER: Built by: 'jenkins'
	08-Jan 12:27:39.101 172.16.2.188:54321    28195 main      INFO WATER: Built on: 'Thu Dec 18 18:54:25 PST 2014'
	08-Jan 12:27:39.101 172.16.2.188:54321    28195 main      INFO WATER: Java availableProcessors: 32
	08-Jan 12:27:39.102 172.16.2.188:54321    28195 main      INFO WATER: Java heap totalMemory: 0.96 gb
	08-Jan 12:27:39.102 172.16.2.188:54321    28195 main      INFO WATER: Java heap maxMemory: 0.96 gb
	08-Jan 12:27:39.102 172.16.2.188:54321    28195 main      INFO WATER: Java version: Java 1.7.0_72 (from Oracle Corporation)
	08-Jan 12:27:39.103 172.16.2.188:54321    28195 main      INFO WATER: OS   version: Linux 3.13.0-43-generic (amd64)
	08-Jan 12:27:39.106 172.16.2.188:54321    28195 main      INFO WATER: Machine physical memory: 251.89 gb
	08-Jan 12:27:39.235 172.16.2.188:54321    28195 main      INFO WATER: ICE root: '/home2/hdp/yarn/usercache/H2O-User/appcache/application_1420450259209_0017'
	08-Jan 12:27:39.238 172.16.2.188:54321    28195 main      INFO WATER: Possible IP Address: br2 (br2), 172.16.2.188
	08-Jan 12:27:39.238 172.16.2.188:54321    28195 main      INFO WATER: Possible IP Address: lo (lo), 127.0.0.1
	08-Jan 12:27:39.330 172.16.2.188:54321    28195 main      INFO WATER: Internal communication uses port: 54322
	+                                                                     Listening for HTTP and REST traffic on  http://172.16.2.188:54321/
	08-Jan 12:27:39.372 172.16.2.188:54321    28195 main      INFO WATER: H2O cloud name: 'H2O_45911'
	08-Jan 12:27:39.372 172.16.2.188:54321    28195 main      INFO WATER: (v2.8.3.2) 'H2O_45911' on /172.16.2.188:54321, discovery address /237.88.97.222:60760
	08-Jan 12:27:39.372 172.16.2.188:54321    28195 main      INFO WATER: If you have trouble connecting, try SSH tunneling from your local machine (e.g., via port 55555):
	+                                                                       1. Open a terminal and run 'ssh -L 55555:localhost:54321 yarn@172.16.2.188'
	+                                                                       2. Point your browser to http://localhost:55555
	08-Jan 12:27:39.377 172.16.2.188:54321    28195 main      DEBG WATER: Announcing new Cloud Membership: [/172.16.2.188:54321]
	08-Jan 12:27:39.379 172.16.2.188:54321    28195 main      INFO WATER: Cloud of size 1 formed [/172.16.2.188:54321 (00:00:00.000)]
	08-Jan 12:27:39.379 172.16.2.188:54321    28195 main      INFO WATER: Log dir: '/home2/hdp/yarn/usercache/H2O-User/appcache/application_1420450259209_0017/h2ologs'
 

""""""""""

- (H2O1) In Terminal, enter ``cd /tmp/h2o-<UserName>/h2ologs`` (where ``<UserName>`` is your computer user name), then enter ``ls -l`` to view a list of the log files. The ``httpd`` log contains the request/response status of all REST API transactions. 
  The rest of the logs use the format ``h2o_\<IPaddress>\_<Port>-<LogLevel>-<LogLevelName>.log``, where ``<IPaddress>`` is the bind address of the H2O instance, ``<Port>`` is the port number, ``<LogLevel>`` is the numerical log level (1-6, with 6 as the highest severity level), and ``<LogLevelName>`` is the name of the log level (trace, debug, info, warn, error, or fatal). 

""""""""""

- (H2O1) Download the logs using R. In R, enter the command ``h2o.downloadAllLogs(client = localH2O,filename = "logs.zip")`` (where ``client`` is the H2O cluster and ``filename`` is the specified filename for the logs).

""""""""

Accessing YARN
---------------

Methods for accessing YARN vary depending on the default management software and version, as well as job status. 

***********
Cloudera 4
***********

1. In Cloudera Manager, click the **yarn** link in the cluster section.
  .. image:: images/Logs_cloudera4_1.png
     :width: 50%

2. Click the **Web UI** drop-down menu. If the job is running, select **ResourceManager Web UI**. If the job is not running, select **HistoryServer Web UI**. 

 .. image:: images/Logs_cloudera4_2.png
    :width: 50%
 
"""""" 
 
***********
Cloudera 5
***********

1. In Cloudera Manager, click the **YARN** link in the cluster section.
  .. image:: images/Logs_cloudera5_1.png
     :width: 50%
  
2. In the Quick Links section, select **ResourceManager Web UI** if the job is running or select **HistoryServer Web UI** if the job is not running. 

 .. image:: images/Logs_cloudera5_2.png
    :width: 50%

""""""""""
 
******* 
Ambari
*******

1. From the Ambari Dashboard, select **YARN**. 

  .. image:: images/Logs_ambari1.png
     :width: 50%

2. From the Quick Links drop-down menu, select **ResourceManager UI**.   

  .. image:: images/Logs_ambari2.png
     :width: 50%

""""""""""


For Non-Hadoop Users
--------------------

*********************
Without Current Jobs
*********************

If you are not using Hadoop and the job is not running: 

- (H2O1) In Terminal, enter ``cd /tmp/h2o-<UserName>/h2ologs`` (where ``<UserName>`` is your computer user name), then enter `ls -l` to view a list of the log files. The ``httpd`` log contains the request/response status of all REST API transactions. 
 The rest of the logs use the format ``h2o_\<IPaddress>\_<Port>-<LogLevel>-<LogLevelName>.log``, where ``<IPaddress>`` is the bind address of the H2O instance, `<Port>` is the port number, ``<LogLevel>`` is the numerical log level (1-6, with 6 as the highest severity level), and ``<LogLevelName>`` is the name of the log level (trace, debug, info, warn, error, or fatal). 

""""""

******************
With Current Jobs
******************

If you are not using Hadoop and the job is still running: 

- (H2O1) Go to the H2O web UI and select **Admin** > **Inspect Log** or go to http://localhost:54321/LogView.html. To download the logs, click the **Download Logs** button. 

	.. Image:: images/Logsdownload.png 
   		:width: 70%

 When you view the log, the output displays the location of log directory after ``Log dir:`` (as shown in the last line in the following example):

::

	08-Jan 13:04:47.018 172.16.2.20:20002     2067  main      INFO WATER: ----- H2O started -----
	08-Jan 13:04:47.018 172.16.2.20:20002     2067  main      INFO WATER: Build git branch: rel-mirzakhani
	08-Jan 13:04:47.018 172.16.2.20:20002     2067  main      INFO WATER: Build git hash: ae31ed04e47d826b73e7180e07ba00db13e879f3
	08-Jan 13:04:47.019 172.16.2.20:20002     2067  main      INFO WATER: Build git describe: jenkins-rel-mirzakhani-2
	08-Jan 13:04:47.019 172.16.2.20:20002     2067  main      INFO WATER: Build project version: 2.8.3.2
	08-Jan 13:04:47.019 172.16.2.20:20002     2067  main      INFO WATER: Built by: 'jenkins'
	08-Jan 13:04:47.019 172.16.2.20:20002     2067  main      INFO WATER: Built on: 'Thu Dec 18 18:54:25 PST 2014'
	08-Jan 13:04:47.019 172.16.2.20:20002     2067  main      INFO WATER: Java availableProcessors: 8
	08-Jan 13:04:47.020 172.16.2.20:20002     2067  main      INFO WATER: Java heap totalMemory: 0.24 gb
	08-Jan 13:04:47.020 172.16.2.20:20002     2067  main      INFO WATER: Java heap maxMemory: 0.89 gb
	08-Jan 13:04:47.020 172.16.2.20:20002     2067  main      INFO WATER: Java version: Java 1.7.0_67 (from Oracle Corporation)
	08-Jan 13:04:47.021 172.16.2.20:20002     2067  main      INFO WATER: OS   version: Mac OS X 10.9.5 (x86_64)
	08-Jan 13:04:47.060 172.16.2.20:20002     2067  main      INFO WATER: Machine physical memory: 16.00 gb
	08-Jan 13:04:47.214 172.16.2.20:20002     2067  main      INFO WATER: ICE root: '/tmp/h2o-H2O-User'
	08-Jan 13:04:47.218 172.16.2.20:20002     2067  main      INFO WATER: Possible IP Address: en0 (en0), fe80:0:0:0:82e6:50ff:fe1e:1480%4
	08-Jan 13:04:47.218 172.16.2.20:20002     2067  main      INFO WATER: Possible IP Address: en0 (en0), 172.16.2.20
	08-Jan 13:04:47.218 172.16.2.20:20002     2067  main      INFO WATER: Possible IP Address: lo0 (lo0), fe80:0:0:0:0:0:0:1%1
	08-Jan 13:04:47.218 172.16.2.20:20002     2067  main      INFO WATER: Possible IP Address: lo0 (lo0), 0:0:0:0:0:0:0:1
	08-Jan 13:04:47.218 172.16.2.20:20002     2067  main      INFO WATER: Possible IP Address: lo0 (lo0), 127.0.0.1
	08-Jan 13:04:47.246 172.16.2.20:20002     2067  main      INFO WATER: Internal communication uses port: 20003
	+                                                                     Listening for HTTP and REST traffic on  http://172.16.2.20:20002/
	08-Jan 13:04:47.286 172.16.2.20:20002     2067  main      INFO WATER: H2O cloud name: 'H2O-User'
	08-Jan 13:04:47.286 172.16.2.20:20002     2067  main      INFO WATER: (v2.8.3.2) 'H2O-User' on /172.16.2.20:20002, discovery address /236.56.107.204:60472
	08-Jan 13:04:47.286 172.16.2.20:20002     2067  main      INFO WATER: If you have trouble connecting, try SSH tunneling from your local machine (e.g., via port 55555):
	+                                                                       1. Open a terminal and run 'ssh -L 55555:localhost:20002 H2O-User@172.16.2.20'
	+                                                                       2. Point your browser to http://localhost:55555
	08-Jan 13:04:47.290 172.16.2.20:20002     2067  main      DEBG WATER: Announcing new Cloud Membership: [/172.16.2.20:20002]
	08-Jan 13:04:47.292 172.16.2.20:20002     2067  main      INFO WATER: Cloud of size 1 formed [/172.16.2.20:20002 (00:00:00.000)]
	08-Jan 13:04:47.292 172.16.2.20:20002     2067  main      INFO WATER: Log dir: '/tmp/h2o-H2O-User/h2ologs'

""""""""""""

- (H2O1) In Terminal, enter ``cd /tmp/h2o-<UserName>/h2ologs`` (where ``<UserName>`` is your computer user name), then enter `ls -l` to view a list of the log files. The ``httpd`` log contains the request/response status of all REST API transactions. 
 The rest of the logs use the format ``h2o_\<IPaddress>\_<Port>-<LogLevel>-<LogLevelName>.log``, where ``<IPaddress>`` is the bind address of the H2O instance, `<Port>` is the port number, ``<LogLevel>`` is the numerical log level (1-6, with 6 as the highest severity level), and ``<LogLevelName>`` is the name of the log level (trace, debug, info, warn, error, or fatal). 

""""""""

- (H2O1) To view the REST API logs from R: 
 1. In R, enter ``h2o.startLogging()``. The output displays the location of the REST API logs: 

:: 
		
	> h2o.startLogging()
	Appending REST API transactions to log file /var/folders/ylcq5nhky53hjcl9wrqxt39kz80000gn/T//RtmpE7X8Yv/rest.log 
		
2. Copy the displayed file path. 
	 Enter ``less`` and paste the file path. 
3. Press Enter. A time-stamped log of all REST API transactions displays. 

	::
		
		------------------------------------------------------------

		Time:     2015-01-06 15:46:11.083
	
		GET       http://172.16.2.20:54321/3/Cloud.json
		postBody: 

		curlError:         FALSE
		curlErrorMessage:  
		httpStatusCode:    200
		httpStatusMessage: OK
		millis:            3

		{"__meta":{"schema_version":	1,"schema_name":"CloudV1","schema_type":"Iced"},"version":"0.1.17.1009","cloud_name":...[truncated]}
		-------------------------------------------------------------
	
""""

- (H2O1) Download the logs using R. In R, enter the command ``h2o.downloadAllLogs(client = localH2O,filename = "logs.zip")`` (where ``client`` is the H2O cluster and ``filename`` is the specified filename for the logs).

""""""""""""""""""""""""


Tunneling between servers with H\ :sub:`2`\ O
"""""""""""""""""""""""""""""""""""""""""""""



1. Use ssh to log in to the machine where H2O will run.
2. Start an instance of H2O by locating the working directory and calling a java command similar to the following example. 

 The port number chosen here is arbitrary; yours may be different.
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

3. Log into the remote machine where the running instance of H2O will be forwarded using a command similar to the following (where users specified port numbers and IP address will be different)

 ::

	ssh -L 55577:localhost:55599 irene@192.168.1.173

4. Check the cluster status.

You are now using H2O from localhost:55577, but the
instance of H2O is running on the remote server (in this
case the server with the ip address 192.168.1.xxx) at port number 55599.

To see this in action note that the web UI is pointed at
localhost:55577, but that the cluster status shows the cluster running
on 192.168.1.173:55599


.. Image:: Clusterstattunnel.png
    :width: 70%
    
    
""""""""""""""""""""""""""""    

Common Troubleshooting Questions
""""""""""""""""""""""""""""""""

**Why is "Upload" is no longer working?**

This can occur when a user’s local disk is full or almost full. 
Free up space on your local disk, and the behavior should resolve. 


""""""""""""""""""""

**What the 'Exclude' field on the Parse page mean?**

In the event a directory rather than a single file is imported, the user can choose certain files to drop or not parse.
All other files in the folder if not specified in the "Exclude" argument are parsed together as a single data object with the common header.

""""""""""""""""""

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

""""""""""""""""""""

**I launched H2O instances on my nodes but why won't they cloud up?**

When launching without specifying the IP address by adding argument -ip:

::

  $ java -Xmx20g -jar h2o.jar -flatfile flatfile.txt -port 54321

and multiple local IP addresses are detected, H2O will fall back to default 127.0.0.1 as shown below:

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
  
  
""""""""""""""""""""""

**Parse Error: "Parser setup appears to be broken, got SVMLight data with (estimated) 0 columns."**

H2O does not currently support a leading label line. Convert a row from:

::

  i 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1
  j 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1

to

::

  1 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1
  2 702101:1 732101:1 803101:1 808101:1 727101:1 906101:1 475101:1

and the file should parse.

""""""""""""""""""""

**How do I export a model with more than 10 trees?**

Please `contact us <support@h2o.ai>`_ for a license that will allow you to run H2O with the following `-license` argument and export larger models.

::

  java -Xmx1g -jar h2o.jar -license h2oeval.asc


""""""""""""""""""""""
