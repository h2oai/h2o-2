#!/bin/bash
CDH3_JOBTRACKER=172.16.2.176:8021
CDH3_NODES=3
H2O_HADOOP=../../h2o-downloaded/hadoop
H2O_JAR=../../h2o-downloaded/h2o.jar

hadoop dfs -rmr /user/kevin/hdfsOutputDirName

echo "You should first check whether you have mapred permissions?"
hadoop jar $H2O_HADOOP/h2odriver_cdh4.jar water.hadoop.h2odriver -jt $CDH3_JOBTRACKER -libjars $H2O_JAR -mapperXmx 8g -nodes 3 -output hdfsOutputDirName -notify h2o_one_node

