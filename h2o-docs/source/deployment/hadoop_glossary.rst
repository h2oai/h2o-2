.. _Hadoop_Glossary:

Hadoop Glossary
===============

**Driver Jar File**
  Jar file that will allow Hadoop to drive an H\ :sub:`2`\ O launch as well as create a connection between HDFS and H\ :sub:`2`\ O for importing data from HDFS.

**H2O**
  H\ :sub:`2`\ O makes Hadoop do math.  H\ :sub:`2`\ O is an Apache v2 licensed open source math and prediction engine.

**H2O Cluster**
  A group of H\ :sub:`2`\ O nodes that operate together to work on jobs.  H\ :sub:`2`\ O scales by distributing work over many H\ :sub:`2`\ O nodes.
  (Note multiple H\ :sub:`2`\ O nodes can run on a single Hadoop node if sufficient resources are available.)  All H\ :sub:`2`\ O nodes in an
  H\ :sub:`2`\ O cluster are peers.  There is no "master" node.

**H2O Key Value**
  H\ :sub:`2`\ O implements a distributed in-memory Key/Value store within the H\ :sub:`2`\ O cluster.  H\ :sub:`2`\ O uses Keys to uniquely
  identify data sets that have been read in (pre-parse), data sets that have been parsed (into HEX format), and models
  (e.g. GLM) that have been created.  For example, when you ingest your data from HDFS into H\ :sub:`2`\ O, that entire data set is
  referred to by a single Key.

**H2O Node**
  H\ :sub:`2`\ O nodes are launched via Hadoop MapReduce and run on Hadoop DataNodes.
  (At a system level, an H\ :sub:`2`\ O node is a Java invocation of h2o.jar.)  Note that while Hadoop operations are centralized
  around HDFS file accesses, H\ :sub:`2`\ O operations are memory-based when possible for best performance.  (H\ :sub:`2`\ O reads the dataset
  from HDFS into memory and then attempts to perform all operations to the data in memory.)

**Hadoop**
  An open source big-data platform. Cloudera, MapR, and Hortonworks are distro providers of Hadoop.
  Data is stored in HDFS (DataNode, NameNode) and processed through MapReduce and managed via JobTracker.

**HDFS**
  Hadoop Distributed File System is a distributed file-system that stores data on commodity machines, providing very high
  aggregate bandwidth across the cluster.

**HEX format**
  The HEX format is an efficient internal representation for data that can be used by H\ :sub:`2`\ O algorithms.
  A data set must be parsed into HEX format before you can operate on it.

**JobTracker**
  The JobTracker is the service within Hadoop that farms out MapReduce tasks to specific nodes in the cluster.

**JobTracker port**
  Port where you can access the JobTracker. The default port might be different for each distribution.

**Mapper Size**
  The memory allocated to each mapper task that will launch on each of the Hadoop Nodes.

**MapReduce**
  MapReduce is Hadoop's programming model for large scale data processing. H\ :sub:`2`\ O nodes are launched via Hadoop MapReduce and run on Hadoop DataNodes.

**Parse**
  The parse operation converts an in-memory raw data set (in CSV format, for example) into a HEX format data set.
  The parse operation takes a data set named by a Key as input, and produces a HEX format Key,Value output.

**Spilling**
  An H\ :sub:`2`\ O node may choose to temporarily "spill" data from memory onto disk.  (Think of this like swapping.)  In Hadoop
  environments, H\ :sub:`2`\ O spills to HDFS.  Usage is intended to function like a temporary cache, and the spilled data is discarded when the job is done.

**YARN**
  A resource-management platform responsible for managing compute resources in clusters and using them for scheduling of users' applications.