.. _Hadoop_Yarn:


H2O on Yarn
===========

When you launch H2O on Hadoop with the ``hadoop jar`` command, Yarn will allocate
the resources necessary to launch the nodes requested. Then, H2O will be launched
as a MapReduce (V2) task such that each Mapper will be a H2O node with the
designated mapper size. A problem that some users might encounter will be Yarn's
rejection of a job request either because the user do not actually have the
machine memory necessary to launch the job or there is a configuration problem.

If the problem is a lack of memory then try launching the job with a less memory.
If the issue is a configuration problem, usually to fix this, the user will need
to adjust the maximum memory that Yarn will allow the user to launch for
each mapper.

**How to launch H2O with less memory?**

Try the job again but this time with a smaller heap size ``-mapperXmx`` and less
nodes ``-nodes`` to verify that a small launch can proceed at all. The default
test case is to run a single 1g node. 

**How to increase the maximum memory Yarn will allow for each mapper?**

If the cluster manager settings are configured for the default maximum memory
size but the memory required for the request exceeds that amount, YARN will not
launch and H2O  will time out. If you have a default configuration, change the
configuration settings in your cluster manager to enable launching of mapper tasks
for specific memory sizes. Use the following formula to calculate the amount of
memory required:

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


``mapreduce.map.memory.mb`` must be less than the YARN memory configuration values for the launch to succeed.  See the examples below for how to change the memory configuration values for your version of Hadoop.


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

