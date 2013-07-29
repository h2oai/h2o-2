#
# For each supported HADOOP_VERSION, add _JAR_DIR and _JARS below,
# and add a line to the build target.
#

cdh3_JAR_DIR=../lib/hadoop/cdh3
cdh3_JARS=$(cdh3_JAR_DIR)/hadoop-core-0.20.2-cdh3u6.jar

cdh4_JAR_DIR=../lib/hadoop/cdh4
cdh4_JARS=$(cdh4_JAR_DIR)/hadoop-common.jar${SEP}$(cdh4_JAR_DIR)/hadoop-mapreduce-client-core-2.0.0-cdh4.2.0.jar

cdh4_yarn_JAR_DIR=../lib/hadoop/cdh4_yarn
cdh4_yarn_JARS=$(cdh4_yarn_JAR_DIR)/common/hadoop-common-2.0.0-cdh4.3.0.jar${SEP}$(cdh4_yarn_JAR_DIR)/mapreduce2/hadoop-mapreduce-client-core-2.0.0-cdh4.3.0.jar

mapr2.1.3_JAR_DIR=../lib/hadoop/mapr2.1.3
mapr2.1.3_JARS=$(mapr2.1.3_JAR_DIR)/hadoop-0.20.2-dev-core.jar

# ------------------------------

# By default, use linux-style separator.
ifndef SEP
    UNAME = $(shell uname)
    ifeq ($(UNAME),Cygwin)
        SEP?=;
    else
        SEP?=:
    endif
endif

COMMON_JARS=../lib/log4j/log4j-1.2.15.jar${SEP}../target/h2o.jar
HADOOP_JARS=$($(HADOOP_VERSION)_JARS)

default: build

build_inner:
	mkdir classes/$(HADOOP_VERSION)
	javac -source 1.6 -target 1.6 -sourcepath src/main/java -classpath "$(COMMON_JARS)${SEP}$(HADOOP_JARS)" -d classes/$(HADOOP_VERSION) src/main/java/water/hadoop/*.java
	jar cf target/h2odriver_$(HADOOP_VERSION).jar -C classes/$(HADOOP_VERSION) .

clean:
	rm -fr classes target

build: clean
	mkdir classes
	mkdir target
	$(MAKE) build_inner HADOOP_VERSION=mapr2.1.3
	$(MAKE) build_inner HADOOP_VERSION=cdh3
	$(MAKE) build_inner HADOOP_VERSION=cdh4
	$(MAKE) build_inner HADOOP_VERSION=cdh4_yarn
	rm -rf ../target/hadoop
	mkdir ../target/hadoop
	cp README.txt ../target/hadoop
	cp target/* ../target/hadoop
