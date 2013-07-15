
COMMON_JARS=lib/log4j-1.2.15.jar:lib/h2o.jar

# Hadoop 1 (CDH3)
HADOOP_JARS=lib/hadoop-0.20.2-core.jar

# Hadoop 2 (CDH4) (classic MapReduce, not YARN)
# HADOOP_JARS=hadoop-2.0.5-alpha/share/hadoop/common/hadoop-common-2.0.5-alpha.jar:hadoop-2.0.5-alpha/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.0.5-alpha.jar:hadoop-2.0.5-alpha/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.0.5-alpha.jar

build: clean
	mkdir classes
	javac -sourcepath src/main/java -classpath $(COMMON_JARS):$(HADOOP_JARS) -d classes src/main/java/water/hadoop/*.java
	mkdir target
	jar cf target/H2ODriver.jar -C classes .

clean:
	rm -fr classes target

