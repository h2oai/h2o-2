#
# For each supported HADOOP_VERSION, add _JAR_DIR and _JARS below,
# and add a line to the build target.
#

cdh3_JAR_DIR=../lib/hadoop/cdh3
cdh3_JARS=$(cdh3_JAR_DIR)/hadoop-core-0.20.2-cdh3u6.jar

cdh4_JAR_DIR=../lib/hadoop/cdh4
cdh4_JARS=$(cdh4_JAR_DIR)/hadoop-common.jar:$(cdh4_JAR_DIR)/hadoop-mapreduce-client-core-2.0.0-cdh4.2.0.jar

# ------------------------------

COMMON_JARS=lib/log4j-1.2.15.jar:lib/h2o.jar
HADOOP_JARS=$($(HADOOP_VERSION)_JARS)

default: build

build_inner:
	mkdir classes/$(HADOOP_VERSION)
	javac -source 1.6 -target 1.6 -sourcepath src/main/java -classpath $(COMMON_JARS):$(HADOOP_JARS) -d classes/$(HADOOP_VERSION) src/main/java/water/hadoop/*.java
	jar cf target/h2odriver_$(HADOOP_VERSION).jar -C classes/$(HADOOP_VERSION) .

clean:
	rm -fr h2o_hadoop
	rm -fr classes target

build: clean
	mkdir classes
	mkdir target
	$(MAKE) build_inner HADOOP_VERSION=cdh3
	$(MAKE) build_inner HADOOP_VERSION=cdh4
	mkdir h2o_hadoop
	cp ../target/h2o.jar h2o_hadoop
	cp README.txt h2o_hadoop
	cp target/* h2o_hadoop
	zip h2o_hadoop.zip h2o_hadoop/*
	mv h2o_hadoop.zip target
	rm -fr h2o_hadoop

