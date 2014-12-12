.. _Javahelp:


Command-line Options
====================

To start an instance of H2O from the command-line, the typical method is to call a Java command similar to `java -Xmx1g -jar h2o.jar`. To customize a running
instance of H2O, change the default values for the options during launch.

There are two different argument types: JVM arguments and H2O arguments. These argument types use the following format: *java* **[JVM OPTIONS]** *-jar h2o.jar* **[H2O OPTIONS]**

""""""

JVM Options
-----------

For standard and advanced JVM options not included here, refer to the available
`VM Options <http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html>`_.
Typically, there are only two commonly-used options for H2O:

    **-version**
        Display Java version info. To make sure the versions of Java and H2O are compatible, use this command to run a check on the Java version. H2O requires at least Java 1.6+.

    -**Xmx<Heap Size>**
          To set the total heap size for an H2O node, configure the  memory allocation option Xmx. By default, this option is set to 1 Gb (-Xmx1g). We recommend launching nodes with a total of four times the memory than your data. *Note: Do not try to launch with more memory than you have available.*
          
""""

H\ :sub:`2`\ O Options
----------------------

To access the following list from the command line, use `-h` or `-help`.

    -h or -help
          Display this help.

    -name <h2oCloudName>
          Cloud name used for discovery of other nodes.
          Nodes with the same cloud name will form an H2O cloud
          (also known as an H2O cluster).

    -flatfile <flatFileName>
          Configuration file specifying H2O cloud node members.

    -ip <ipAddressOfNode>
          IP address of the specified node.

    -port <port>
          Port number for the specified node (Note: port+1 is also used).
          (The default port is 54321.)

    -network <IPv4network1Specification>[,<IPv4network2Specification> ...]
          The IP address discovery code uses the first interface
          that matches one of the networks in the comma-separated list.
          Use this command instead of -ip for a broad range of addresses.
          (Example network specification: '10.1.2.0/24' allows 256 legal
          possibilities.)

    -ice_root <fileSystemPath>
          The directory where H2O spills temporary data to disk.
          (The default is '/tmp/h2o-<UserName>'.)

    **-single_precision**
          Reduce the maximum (storage) precision for floating point numbers
          from double to single precision to save memory of numerical data.
          (The default is double precision.)

    -nthreads <# of threads>
          Maximum number of threads in the low priority batch-work queue.
          (The default is 4*numcpus.)

    -license <licenseFilePath>
          Path to license file on local filesystem.
          
    \-version
    	Display version info.       
          
""""          

Cloud formation behavior
------------------------

New H2O nodes join together to form a cloud at startup.
Once a cloud is assigned work, it locks out new members
from joining the cloud. H2O works best on distributed multinode clusters
that are configured similarly and have equal amounts of allocated memory.

**Examples:**

  Start an H2O node with 4GB of memory and a *default cloud name:*
      $ java -Xmx4g -jar h2o.jar

  Start an H2O node with 6GB of memory and a *specified cloud name* (where *<CloudName>* is the specified name):
      $ java -Xmx6g -jar h2o.jar -name *<CloudName>*

  Start an H2O cloud with three 2GB nodes and a *specified cloud name* (where *<CloudName>* is the specified name):
      $ java -Xmx2g -jar h2o.jar -name *<CloudName>*
      $ java -Xmx2g -jar h2o.jar -name *<CloudName>*
      $ java -Xmx2g -jar h2o.jar -name *<CloudName>*
