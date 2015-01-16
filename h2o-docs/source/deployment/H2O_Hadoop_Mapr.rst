H2O on Hadoop with MapR 4.0.1
=============================

**Note**: Verify that you have permissions to dispatch YARN jobs and write to HDFS on the Hadoop cluster before installing H2O. 

All mappers must be able to communicate with each other and need to run at the same time. 

""""""""""

Installation
------------

1. Log in to the Hadoop cluster: 

	``ssh <username>@<HadoopClusterName>``

	If you are asked if you want to continue connecting, enter ``yes``.
2. Enter the following: 

	``wget http://h2o-release.s3.amazonaws.com/h2o/SUBST_RELEASE_NAME/SUBST_BUILD_NUMBER/h2o-SUBST_PROJECT_VERSION.zip``
	
3. Wait while H2O downloads - the progress bar indicates completion. 

	``100%[=================================>] 140,951,040 2.23M/s   in 65s``
	
4. 	Enter the following: 

	``unzip h2o-SUBST_PROJECT_VERSION.zip``
	
5. Wait while the H2O package unzips. 
6. On the Hadoop node, change the current directory to the location of the Hadoop and H2O driver jar files: 

	``cd h2o-SUBST_PROJECT_VERSION/hadoop``
	
7. Enter the following: 

	``hadoop jar h2odriver_mapr4.0.1.jar water.hadoop.h2odriver -libjars ../h2o.jar -mapperXmx 1g -nodes 1 -output <hdfsOutputDirName>``

	**Note**: You may need to configure the following parameters: 
	
	-``-mapperXmx``: Specify the amount of memory (in GB) allocated to H2O. We recommend configuring at least four times the size of your dataset, but not more than is available on your system. 
	
	-``-nodes``: Specify the number of nodes.
	 
	-``-output``: Specify a unique name for the output (``<hdfsOutputDirName>`` in the example). 

8. To use the web UI, use your browser to access any of the nodes H2O launches. The IP addresses of the nodes display in the output (look for ``H2O node <IPaddress> reports H2O cluster size <#>``): 

::

	Determining driver host interface for mapper->driver callback...
	[Possible callback IP address: 172.16.2.181]
	[Possible callback IP address: 127.0.0.1]
	...
	Waiting for H2O cluster to come up...
	H2O node 172.16.2.184:54321 requested flatfile
	Sending flatfiles to nodes...
	 [Sending flatfile to node 172.16.2.184:54321]
	H2O node 172.16.2.184:54321 reports H2O cluster size 1 
	H2O cluster (1 nodes) is up
	Blocking until the H2O cluster shuts down...

	
""""""""

Monitoring Jobs
---------------

Use the standard JobTracker web UI (``http://<JobTrackerIP>:<default_port>``). Different distributions can have different default JobTracker web UI ports. 
The default JobTracker web UI port for MapR is 50030. 

Because of the way H2O works with Hadoop, most H2O tasks display in the Hadoop manager as ``RUNNING``, ``FAILED``, or ``KILLED``. ``RUNNING`` means the job is currently in progress. ``FAILED`` means that the job was unsuccessful (for example, due to denied permissions). ``KILLED`` means that the H2O cluster was shutdown. If you shut down H2O from the web UI (**Admin** > **Shutdown**), then ``SUCCESSFUL`` displays in the Hadoop manager. Individual tasks, such as creating a model, do not display in the Hadoop manager. 

""""""""

Shutting Down Clusters
----------------------

From the H2O web UI (``http://<H2O_node>:54321``), click the drop-down **Admin** menu and select **Shutdown**. 

You can also use the ``hadoop job -kill`` command. 

""""""""""

Getting Help
------------

- To view information about parameters and usage, use: ``hadoop jar h2odriver_mapr4.0.1.jar water.hadoop.h2odriver -libjar ../h2o.jar -help``

- If the mapper task fails, do not retry the mapper task. Restart the H2O cloud. 
- To get the Hadoop-level logs, use: ``$yarn logs -applicationId <application_id>``

	