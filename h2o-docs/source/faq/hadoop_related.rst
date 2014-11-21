.. _Hadoop_Related:


Hadoop and H\ :sub:`2`\ O
=========================

Sending YARN Logs for Hadoop Troubleshooting
""""""""""""""""""""""""""""""""""""""""""""
If H\ :sub:`2`\ O does not launch properly on Hadoop, send us the YARN logs.

When launching H\ :sub:`2`\ O on Hadoop, the following messages display regardless of launch failure or success. If these messages do not display, the argument has not been entered correctly:

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


To view the YARN log, execute the command specified in the next to last line in the previous example ("For YARN users, logs command is '...'). The command will be unique for each instance. 

::

  yarn logs -applicationId application_201407040936_0030

Copy and email the logs to support@0xdata.com or paste to h2ostream@googlegroups.com with a brief
description of your Hadoop environment, including the Hadoop distribution and version.


Common Hadoop Questions
""""""""""""""""""""""""

**What versions of Hadoop are supported?**

Currently, the major versions that H2O supports are HDP 1.3 and HDP 2.1. H2O also supports MapR 2.1 and 3.1, as well as CDH 4 and 5. 

---


**What's the syntax for the file path of a data set sitting in hdfs?**

To locate an HDFS file, go to **Data > Import** and enter **hdfs://** in the **path** field. H\ :sub:`2`\ O automatically detects any HDFS paths. This is a good way to verify the path to your data set before importing through R or any other non-web API.

---

**When interacting with an H\ :sub:`2`\ O cluster launched on multiple Hadoop nodes, is it necessary for R to be installed on all the data nodes?**

No - as long as the R instance can communicate with one of the nodes in the network, R can be installed on any of the nodes, or even on a local machine that will securely tunnel into the cluster.

---

**Is it possible to launch the H\ :sub:`2`\ O cluster on Hadoop nodes using R’s** `h2o.init()` **command?**

No - follow the instructions in :ref:`Hadoop_Tutorial` and add the IP address to the `h2o.init()` function to connect to the cluster.

---

**What does** `"ERROR: Output directory hdfs://sandbox.hortonworks.com:8020/user/root/hdfsOutputDir already exists?"` **mean?**

Each mapper task gets its own output directory in HDFS. To prevent overwriting multiple users' files, each mapper task must have a unique output directory name. Change
the `-output hdfsOutputDir` argument to `-output hdfsOutputDir1` and the task should launch.

---

**What should I do if H\ :sub:`2`\ O  starts to launch but times out in 120 seconds?**


1. YARN or MapReduce's configuration is not configured correctly. Enable launching for mapper tasks of specified memory sizes. If YARN only allows mapper tasks with a maximum memory size of 1g and the request requires 2g, then the request will timeout at the default of 120 seconds. Read `Configuration Setup <http://hortonworks.com/blog/how-to-plan-and-configure-yarn-in-hdp-2-0/>`_ to make sure your setup will run.

2. The nodes are not communicating with each other. If you request a cluster of two nodes and the output shows a stall in reporting the other nodes and forming a cluster (as shown in the following example), check that the security settings for the network connection between the two nodes are not preventing the nodes from communicating with each other. You should also check to make sure that the flatfile that is generated and being passed has the correct home address; if there are multiple local IP addresses, this could be an issue.


::

  $ hadoop jar h2odriver_horton.jar water.hadoop.h2odriver -libjars ../h2o.jar
  -driverif 10.115.201.59 -timeout 1800 -mapperXmx 1g -nodes 2 -output hdfsOutputDirName
    13/10/17 08:51:14 INFO util.NativeCodeLoader: Loaded the native-hadoop library
    13/10/17 08:51:14 INFO security.JniBasedUnixGroupsMapping: Using JniBasedUnixGroupsMapping for
    Group resolution
    Using mapper->driver callback IP address and port: 10.115.201.59:34389
    (You can override these with -driverif and -driverport.)
    Driver program compiled with MapReduce V1 (Classic)
    Memory Settings:
        mapred.child.java.opts:      -Xms1g -Xmx1g
        mapred.map.child.java.opts:  -Xms1g -Xmx1g
        Extra memory percent:        10
        mapreduce.map.memory.mb:     1126
    Job name 'H2O_61026' submitted
    JobTracker job ID is 'job_201310092016_36664'
    Waiting for H2O cluster to come up...
    H2O node 10.115.57.45:54321 requested flatfile
    H2O node 10.115.5.25:54321 requested flatfile
    Sending flatfiles to nodes...
        [Sending flatfile to node 10.115.57.45:54321]
        [Sending flatfile to node 10.115.5.25:54321]
    H2O node 10.115.57.45:54321 reports H2O cluster size 1
    H2O node 10.115.5.25:54321 reports H2O cluster size 1
    
---

**What should I do if the H2O job launches but  terminates after 600 seconds?**

The likely cause is a driver mismatch - check to make sure the Hadoop distribution matches the driver jar file used to launch H\ :sub:`2`\ O. If your distribution is not currently
available in the package, `email us <support@0xdata>`_ for a new driver file.

---

**What should I do if I want to create a job with a bigger heap size but YARN doesn't launch and H\ :sub:`2`\ O times out?**

First, try the job again but with a smaller heap size (`-mapperXmx`) and a smaller number of nodes (`-nodes`) to verify that a small launch can proceed at all.

If the cluster manager settings are configured for the default maximum memory size but the memory required for the request exceeds that amount, YARN will not launch and H\ :sub:`2`\ O  will time out. 
If you have a default configuration, change the configuration settings in your cluster manager to enable launching of mapper tasks for specific memory sizes. Use the following formula to calculate the amount of memory required: 

::

    YARN container size 
    == mapreduce.map.memory.mb
    == mapperXmx + (mapperXmx * extramempercent [default is 10%])

Output from an H2O launch is shown below:

::

    $ hadoop jar h2odriver_hdp2.1.jar water.hadoop.h2odriver 
    -libjars ../h2o.jar -mapperXmx 30g -extramempercent 20 -nodes 4 -output hdfsOutputDir
    Determining driver host interface for mapper->driver callback...
    [Possible callback IP address: 172.16.2.181]
    [Possible callback IP address: 127.0.0.1]
    Using mapper->driver callback IP address and port: 172.16.2.181:58280
    (You can override these with -driverif and -driverport.)
    Driver program compiled with MapReduce V1 (Classic)
    14/10/10 18:39:53 INFO Configuration.deprecation: mapred.map.child.java.opts is deprecated.
     Instead, use mapreduce.map.java.opts
    Memory Settings:
    mapred.child.java.opts:      -Xms30g -Xmx30g
    mapred.map.child.java.opts:  -Xms30g -Xmx30g
    Extra memory percent:        20
    mapreduce.map.memory.mb:     36864


`mapreduce.map.memory.mb` must be less than the YARN memory configuration values for the launch to succeed.  See the examples below for how to change the memory configuration values for your version of Hadoop.


**For Cloudera, configure the settings in Cloudera Manager. Depending on how the cluster is configured, you may need to change the settings for more than one role group.**
	
1. Click **Configuration** and enter the following search term in quotes: **yarn.nodemanager.resource.memory-mb**.

2. Enter the amount of memory (in GB) to allocate in the **Value** field. If more than one group is listed, change the values for all listed groups.
	
	.. image:: TroubleshootingHadoopClouderayarnnodemgr.png
	   :width: 100 %	
	
3. Click the **Save Changes** button in the upper-right corner. 
4. Enter the following search term in quotes: **yarn.scheduler.maximum-allocation-mb**
5. Change the value, click the **Save Changes** button in the upper-right corner, and redeploy.
	
	.. image:: TroubleshootingHadoopClouderayarnscheduler.png
	   :width: 100%
			
	
**For Hortonworks,** `configure <http://docs.hortonworks.com/HDPDocuments/Ambari-1.6.0.0/bk_Monitoring_Hadoop_Book/content/monitor-chap2-3-3_2x.html>`_ **the settings in Ambari.**

1. Select **YARN**, then click the **Configs** tab. 
2. Select the group. 
3. In the **Node Manager** section, enter the amount of memory (in MB) to allocate in the **yarn.nodemanager.resource.memory-mb** entry field. 
	
	.. image:: TroubleshootingHadoopAmbariNodeMgr.png
	  :width: 100 %
	  
4. In the **Scheduler** section, enter the amount of memory (in MB)to allocate in the **yarn.scheduler.maximum-allocation-mb** entry field. 
	
	.. image:: TroubleshootingHadoopAmbariyarnscheduler.png
	  :width: 100 %

5. 	Click the **Save** button at the bottom of the page and redeploy the cluster. 
	

**For MapR:**

1. Edit the **yarn-site.xml** file for the node running the ResourceManager. 
2. Change the values for the `yarn.nodemanager.resource.memory-mb` and `yarn.scheduler.maximum-allocation-mb` properties.
3. Restart the ResourceManager and redeploy the cluster. 
	

To verify the values were changed, check the values for the following properties:
 	
	 - `<name>yarn.nodemanager.resource.memory-mb</name>`
	 - `<name>yarn.scheduler.maximum-allocation-mb</name>`
	
---
