.. _Javahelp:


Command-line Options
====================

When an instance of H\ :sub:`2`\ O is started from the command-line, users
generally call a Java command similar to "java -Xmx1g -jar
h2o.jar " Users can customize their running
instance of H\ :sub:`2`\ O by changing options from their default value during launch.

There are two different argument types: JVM arguments and H\ :sub:`2`\ O arguments that follows
the following format when ran: *java* **[JVM OPTIONS]** *-jar h2o.jar* **[H2O OPTIONS]**


JVM Options
-----------

For other common as well as advance JVM options not mentioned below search through available
`VM Options <http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html>`_.
Typically there are only two options used commonly for H\ :sub:`2`\ O's case.

    **-version**
        Print java version info and exit. Run a check on the java version to make sure the Java and  H\ :sub:`2`\ O's versions are compatible. In particular H\ :sub:`2`\ O supports Java 1.6+.

    -Xmx<Heap Size>
          Is a memory allocation option that sets the total heap size for a H\ :sub:`2`\ O node. By default
          this option is set to -Xmx1g. It is recommended to launch nodes with a total of four times the
          memory than your data. *Note: Do not try to launch with more memory than you have available.*

H\ :sub:`2`\ O Options
----------------------

To access the following list from the command line use option -h or -help.

    -h | -help
          Print this help.

    **-version**
          Print version info and exit.

    -name <h2oCloudName>
          Cloud name used for discovery of other nodes.
          Nodes with the same cloud name will form an H2O cloud
          (also known as an H2O cluster).

    -flatfile <flatFileName>
          Configuration file explicitly listing H2O cloud node members.

    -ip <ipAddressOfNode>
          IP address of this node.

    -port <port>
          Port number for this node (note: port+1 is also used).
          (The default port is 54321.)

    -network <IPv4network1Specification>[,<IPv4network2Specification> ...]
          The IP address discovery code will bind to the first interface
          that matches one of the networks in the comma-separated list.
          Use instead of -ip when a broad range of addresses is legal.
          (Example network specification: '10.1.2.0/24' allows 256 legal
          possibilities.)

    -ice_root <fileSystemPath>
          The directory where H2O spills temporary data to disk.
          (The default is '/tmp/h2o-Amy'.)

    **-single_precision**
          Reduce the max. (storage) precision for floating point numbers
          from double to single precision to save memory of numerical data.
          (The default is double precision.)

    -nthreads <# of threads>
          Maximum number of threads in the low priority batch-work queue.
          (The default is 4*numcpus.)

    -license <licenseFilePath>
          Path to license file on local filesystem.

Cloud formation behavior
------------------------

New H\ :sub:`2`\ O nodes join together to form a cloud at startup time.
Once a cloud is given work to perform, it locks out new members
from joining. H\ :sub:`2`\ O works best on distributed multinode clusters
when the clusters are similar in configuration, and allocated
equal amounts of memory.

**Examples:**

  Start an H\ :sub:`2`\ O node with 4GB of memory and a *default cloud name:*
      $ java -Xmx4g -jar h2o.jar

  Start an H\ :sub:`2`\ O node with 6GB of memory and a *specified cloud name:*
      $ java -Xmx6g -jar h2o.jar -name TomsCloud

  Start an H\ :sub:`2`\ O cloud with three 2GB nodes and a *specified cloud name:*
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
