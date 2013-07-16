# -----
# Set version of Cloudera to build for below.
# Override HADOOP_JARS directly below for a different hadoop build.

CDH_VERSION=4

# -----

COMMON_JARS=lib/log4j-1.2.15.jar:lib/h2o.jar

CDH3_JAR_DIR=../lib/hadoop/cdh3
CDH3_JARS=$(CDH3_JAR_DIR)/hadoop-core-0.20.2-cdh3u6.jar

CDH4_JAR_DIR=../lib/hadoop/cdh4
CDH4_JARS=$(CDH4_JAR_DIR)/hadoop-common.jar:$(CDH4_JAR_DIR)/hadoop-mapreduce-client-core-2.0.0-cdh4.2.0.jar

HADOOP_JARS=$(CDH$(CDH_VERSION)_JARS)

build: clean
	mkdir classes
	javac -sourcepath src/main/java -classpath $(COMMON_JARS):$(HADOOP_JARS) -d classes src/main/java/water/hadoop/*.java
	mkdir target
	jar cf target/H2ODriver.jar -C classes .

clean:
	rm -fr classes target

