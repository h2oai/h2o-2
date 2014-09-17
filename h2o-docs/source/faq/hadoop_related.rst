.. _Hadoop_Related:


Hadoop and H\ :sub:`2`\ O
=========================

Hadoop - Copy and Paste Your YARN Logs
""""""""""""""""""""""""""""""""""""""
In the event H\ :sub:`2`\ O fails to launch properly on Hadoop send us the YARN logs.

When launching H\ :sub:`2`\ O on Hadoop the following messages will show up first regardless of failure or success, otherwise the argument has not been entered correctly:

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


To view the YARN log execute the command specify on line "For YARN users, logs command is <>"

::

  yarn logs -applicationId application_201407040936_0030

Copy and email the logs to support@0xdata.com or paste to h2ostream@googlegroups.com with a brief
description of your Hadoop environment including the distribution and version of Hadoop.


Common Hadoop Issues
""""""""""""""""""""
**What's the syntax for the file path of a data set sitting in hdfs?**

To locate a file sitting in hdfs go to Data>Import and enter in the path hdfs:// and H\ :sub:`2`\ O will automatically detect any hdfs paths,
this is a good way to verify the path to your data set before importing through R or any other non web API.

**When interacting with H2O cluster launched on multiple Hadoop nodes, is it necessary for R to be installed on all the data nodes?**

No, so long as the R instance can talk to one of the nodes in the network, R can be installed on any of the nodes or even on your local machine that will securely tunnel into the cluster.

**Is it possible to launch the H2O cluster on Hadoop nodes using R’s h2o.init() command?**

No, simply follow the :ref:`Hadoop_Tutorial` and connect to the cluster by adding the IP address to the h2o.init() function.

**The H2O job launches but is terminating after 600 seconds?**

This points to a driver mismatch, check to make sure the distribution of Hadoop used matches the driver jar file used to launch H\ :sub:`2`\ O. If the distributions you are using is not currently
available in the package, `email us <support@0xdata>`_ for a new driver file.

**ERROR: Output directory hdfs://sandbox.hortonworks.com:8020/user/root/hdfsOutputDir already exists?**

Each mapper task gets its own output directory in HDFS. To prevent overwriting multiple users' files each mapper task has to have a unique output directory name; simply change
the -output hdfsOutputDir argument to -output hdfsOutputDir1 and the task should launch.

**H2O starts to launch but timesout in 120 seconds?**

#. YARN or MapReduce's configuration was set improperly. Make sure to allow mapper task of certain memory sizes to launch. If YARN is only allowing mapper task of maximum memory size of 1g and the user ask for 2g then the request will timeout at a default 120 seconds. Read `Configuration Setup <http://hortonworks.com/blog/how-to-plan-and-configure-yarn-in-hdp-2-0/>`_ to make sure your setup will run.

#. The nodes are not talking to each other. If the user ask for a cluster of two nodes and the output shows that there is a stall in reporting the other nodes and not forming a cluster:

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

Check to make sure the network connection between the two nodes doesn't have any security settings preventing nodes from talking to each other. Also check to make sure that the flatfile generated and being passed
has the correct home address; if there are multiple local ip address this could be an issue.