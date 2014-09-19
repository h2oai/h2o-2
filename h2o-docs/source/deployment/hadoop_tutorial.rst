.. _Hadoop_Tutorial:

Running H\ :sub:`2`\ O on Hadoop
================================

The following tutorial will walk the user through the download or build of H\ :sub:`2`\ O and the parameters involved
in launching H\ :sub:`2`\ O from the command line.

**Step 1**

Either download the latest H\ :sub:`2`\ O release from our `download page <http://0xdata.com/download//>`_ or download our
source code and build the code from git by running:

::
  
  $ git clone https://github.com/0xdata/h2o.git
  $ cd h2o
  $ make

**Step 2**

Copy the build output or downloaded zip file to a Hadoop Node where the user will be running hadoop commands.

::

  $ scp target/h2o-<h2o_version>.zip <location that Hadoop command will be executed>
  or
  $ scp h2o-<hadoop_version>.zip <location that Hadoop command will be executed>


**Step 3**

Prepare job input on Hadoop Node by unzipping the build file and changing to the directory with the Hadoop and H\ :sub:`2`\ O's driver jar files.

::

  $ unzip h2o-<h2o_version>.zip
  $ cd h2o-<h2o_version>/hadoop


**Step 4**

To launch H\ :sub:`2`\ O nodes and form a cluster on the Hadoop cluster run:

::

  $ hadoop jar <h2o_driver_jar_file> water.hadoop.h2odriver [-jt <jobtracker:port>]
  -libjars ../h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName


*Parameters*
  h2o_driver_jar_file : For each major release of each distribution of hadoop, there is a driver jar file that the user will need to launch H\ :sub:`2`\ O with.
  Currently available driver jar files in each build of H\ :sub:`2`\ O include h2odriver_cdh5.jar, h2odriver_hdp2.1.jar, and mapr2.1.3.jar.

  jobtracker:port : The argument is optional and typically without it the jobtracker will be available at the default port of each distro.

  mapperXmx : The mapper size or the amount of memory allocated to each node.

  nodes : The number of nodes requested to form the cluster.

  output : The name of the directory created for each mapper task which has to be unique to each instance of H\ :sub:`2`\ O since they cannot be overwritten.

**Step 5**

To monitor your job simply direct your web browser to your standard job tracker Web UI.
To access H\ :sub:`2`\ O's Web UI direct your web browser to one of the instances launched. If you are unsure where your port is launched
review the output from your command.

.. image:: hadoop_cmd_output.png
    :width: 100 %
