.. _Hadoop_Tutorial:

Running H\ :sub:`2`\ O on Hadoop
================================

The following tutorial will walk the user through the download or build of H\ :sub:`2`\ O and the parameters involved in launching H\ :sub:`2`\ O from the command line.


1. Download the latest H\ :sub:`2`\ O release:

::
  
  $ wget http://h2o-release.s3.amazonaws.com/h2o/rel-mandelbrot/1/h2o-2.8.0.1.zip
 

2. Prepare the job input on the Hadoop Node by unzipping the build file and changing to the directory with the Hadoop and H\ :sub:`2`\ O's driver jar files.

::

  $ unzip h2o-2.8.0.1.zip
  $ cd h2o-2.8.0.1/hadoop



3. To launch H\ :sub:`2`\ O nodes and form a cluster on the Hadoop cluster, run:

::

  $ hadoop jar <h2o_driver_jar_file> water.hadoop.h2odriver [-jt <jobtracker:port>]
  -libjars ../h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName


4. To monitor your job, direct your web browser to your standard job tracker Web UI.
To access H\ :sub:`2`\ O's Web UI, direct your web browser to one of the launched instances. If you are unsure where your port is launched,
review the output from your command.

.. image:: hadoop_cmd_output.png
    :width: 100 %
    
    
**Parameters**

`h2o_driver_jar_file `: For each major release of each distribution of hadoop, there is a driver jar file that the user will need to launch H2O with. Currently available driver jar files in each build of H2O include `h2odriver_cdh5.jar`, `h2odriver_hdp2.1.jar`, and `mapr2.1.3.jar`.


`jobtracker:port `: The argument is optional and typically without it the jobtracker will be available at the default port of each distro.


`mapperXmx` : The mapper size or the amount of memory allocated to each node.


`nodes` : The number of nodes requested to form the cluster.


`output` : The name of the directory created for each mapper task which has to be unique to each instance of H2O since they cannot be overwritten.

