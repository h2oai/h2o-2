
This package contains:

    README.txt               The file you are reading right now.
    LICENSE.txt              This software is released under the Apache v2.0
                             open source license.
    h2o.jar                  The H2O engine.
    h2o-sources.jar          Source code to compile h2o.jar.

    R/                       Directory containing H2O package for R.
    R/README.txt             Read this if you are interested in writing R
                             programs that interact with H2O.
    R/h2o*.tar.gz            The R package for you to install.

    hadoop/                  Directory containing hadoop integration for H2O.
    hadoop/README.txt        Read this if you want to run H2O on a hadoop
                             cluster.
    hadoop/h2odriver_*.jar   Driver programs to start h2o.jar as a MapReduce
                             job on a Hadoop cluster.  Choose the right one
                             for your version of Hadoop (please contact 
                             0xdata if your version is not here yet).

    ec2/                     Directory containing scripts for Amazon EC2.
    ec2/README.txt           Read this for instructions on how to run H2O
                             on EC2.

    spark/                   Directory containing information about how to
                             find Sparkling Water.

    tableau/                 Directory containing example Tableau workbooks
                             for H2O.


To try out H2O from the command line, type: 
    $ java -Xmx2g -jar h2o.jar

and then go to this URL to visit the H2O Web UI:
    http://localhost:54321


For further documentation, including how to use H2O from R, please visit:
    http://docs.0xdata.com


Thanks,
The H2O team
www.0xdata.com

