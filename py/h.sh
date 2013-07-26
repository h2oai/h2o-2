


hadoop dfs -rmr /user/kevin/hdfsOutputDir
hadoop jar ../hadoop/target/h2odriver_cdh3.jar water.hadoop.h2odriver -jt 192.168.1.180:50030 -files flatfile.txt -libjars ../target/h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDir
      

# Job name 'H2O_44047' submitted
# JobTracker job ID is 'job_201307242013_0001'

# hadoop job -status <job-id>
# hadoop job -kill <job-id>
hadoop job -list all
# hadoop job -history all <jobOutputDir>

# 13/07/26 16:39:06 ERROR security.UserGroupInformation: PriviledgedActionException as:kevin (auth:SIMPLE) cause:java.io.IOException: Call to /192.168.1.180:50030 failed on local exception: java.io.EOFException
# ERROR: Call to /192.168.1.180:50030 failed on local exception: java.io.EOFException
# java.io.IOException: Call to /192.168.1.180:50030 failed on local exception: java.io.EOFException
#     at org.apache.hadoop.ipc.Client.wrapException(Client.java:1187)
# 
