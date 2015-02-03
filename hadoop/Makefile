#
# For each supported HADOOP_VERSION, add _JAR_DIR and _JARS below,
# and add a line to the build target.
#

cdh3_JAR_DIR=../lib/hadoop/cdh3
cdh3_JARS=$(cdh3_JAR_DIR)/hadoop-core-0.20.2-cdh3u6.jar

cdh4_JAR_DIR=../lib/hadoop/cdh4
cdh4_JARS=$(cdh4_JAR_DIR)/hadoop-common.jar${SEP}${cdh4_JAR_DIR}/hadoop-annotations-2.0.0-cdh4.2.0.jar${SEP}$(cdh4_JAR_DIR)/hadoop-mapreduce-client-core-2.0.0-cdh4.2.0.jar

cdh5_JAR_DIR=../lib/hadoop/cdh5
cdh5_JARS=$(cdh5_JAR_DIR)/hadoop-common-2.2.0-cdh5.0.0-beta-2.jar${SEP}${cdh5_JAR_DIR}/hadoop-annotations-2.2.0-cdh5.0.0-beta-2.jar${SEP}$(cdh5_JAR_DIR)/hadoop-mapreduce-client-core-2.2.0-cdh5.0.0-beta-2.jar

cdh4_yarn_JAR_DIR=../lib/hadoop/cdh4_yarn
cdh4_yarn_JAR_DIR=../lib/hadoop/cdh4_yarn
cdh4_yarn_JARS=$(cdh4_yarn_JAR_DIR)/common/hadoop-common-2.0.0-cdh4.3.0.jar${SEP}${cdh4_yarn_JAR_DIR}/common/lib/hadoop-annotations-2.0.0-cdh4.3.0.jar${SEP}$(cdh4_yarn_JAR_DIR)/mapreduce2/hadoop-mapreduce-client-core-2.0.0-cdh4.3.0.jar

mapr2.1.3_JAR_DIR=../lib/hadoop/mapr2.1.3
mapr2.1.3_JARS=$(mapr2.1.3_JAR_DIR)/hadoop-0.20.2-dev-core.jar

mapr3.1.1_JAR_DIR=../lib/hadoop/mapr3.1.1
mapr3.1.1_JARS=$(mapr3.1.1_JAR_DIR)/hadoop-0.20.2-dev-core.jar

mapr4.0.1_JAR_DIR=../lib/hadoop/mapr4.0.1
mapr4.0.1_JARS=$(mapr4.0.1_JAR_DIR)/hadoop-annotations-2.4.1-mapr-1408.jar${SEP}$(mapr4.0.1_JAR_DIR)/hadoop-common-2.4.1-mapr-1408.jar${SEP}${mapr4.0.1_JAR_DIR}/hadoop-mapreduce-client-core-2.4.1-mapr-1408.jar

hdp1.3.2_JAR_DIR=../lib/hadoop/hdp1.3.2
hdp1.3.2_JARS=$(hdp1.3.2_JAR_DIR)/hadoop-core-1.2.0.1.3.2.0-111.jar

hdp2.0.6_JAR_DIR=../lib/hadoop/hdp2.0.6
hdp2.0.6_JARS=$(hdp2.0.6_JAR_DIR)/hadoop-common-2.2.0.2.0.6.0-101.jar${SEP}$(hdp2.0.6_JAR_DIR)/hadoop-mapreduce-client-core-2.2.0.2.0.6.0-101.jar${SEP}${hdp2.0.6_JAR_DIR}/hadoop-annotations-2.2.0.2.0.6.0-101.jar

hdp2.1_JAR_DIR=../lib/hadoop/hdp2.1
hdp2.1_JARS=$(hdp2.1_JAR_DIR)/hadoop-common-2.4.0.2.1.1.0-385.jar${SEP}$(hdp2.1_JAR_DIR)/hadoop-mapreduce-client-core-2.4.0.2.1.1.0-385.jar${SEP}${hdp2.1_JAR_DIR}/hadoop-annotations-2.4.0.2.1.1.0-385.jar

hdp2.2_JAR_DIR=../lib/hadoop/hdp2.2
hdp2.2_JARS=$(hdp2.2_JAR_DIR)/hadoop-common-2.6.0.2.2.0.0-2041.jar${SEP}$(hdp2.2_JAR_DIR)/hadoop-mapreduce-client-core-2.6.0.2.2.0.0-2041.jar${SEP}${hdp2.2_JAR_DIR}/hadoop-annotations-2.6.0.2.2.0.0-2041.jar

# ------------------------------

# Figure out classpath separator for Unix vs. Windows
ifndef SEP
    UNAME = $(shell uname)
    ifeq ($(UNAME),Darwin)
        SEP=:
    endif
    ifeq ($(UNAME),Linux)
        SEP=:
    endif

    ifndef SEP
        UNAME_DASH_O = $(shell uname -o)
        ifeq ($(UNAME_DASH_O),Cygwin)
            SEP?=;
        else
            SEP?=:
        endif
    endif
endif

COMMON_JARS=../lib/log4j/log4j-1.2.15.jar${SEP}../target/h2o.jar
HADOOP_JARS=$($(HADOOP_VERSION)_JARS)

default: build

F=src/main/java/water/hadoop/h2odriver_config.java
build_inner:
	rm -f $(F)
	@echo "package water.hadoop;" >> $(F)
	@echo "class h2odriver_config {" >> $(F)
	@echo "    public static boolean usingYarn() { return $(YARN); }" >> $(F)
	@echo "    public static boolean overrideLog4jInit() { return $(L4JOVR); }" >> $(F)
	@echo "}" >> $(F)
	mkdir classes/$(HADOOP_VERSION)
	javac -Xlint:-deprecation -source 1.6 -target 1.6 -sourcepath src/main/java -classpath "$(COMMON_JARS)${SEP}$(HADOOP_JARS)" -d classes/$(HADOOP_VERSION) src/main/java/water/hadoop/*.java
	jar cf target/h2odriver_$(HADOOP_VERSION).jar -C classes/$(HADOOP_VERSION) .

clean:
	rm -fr classes target
	rm -f src/main/java/water/hadoop/h2odriver_config.java

build: clean
	mkdir classes
	mkdir target
	$(MAKE) build_inner HADOOP_VERSION=mapr2.1.3 YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=mapr3.1.1 YARN=false L4JOVR=true
	$(MAKE) build_inner HADOOP_VERSION=mapr4.0.1 YARN=false L4JOVR=true
	$(MAKE) build_inner HADOOP_VERSION=cdh3      YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=cdh4      YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=cdh4_yarn YARN=true  L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=cdh5      YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=hdp1.3.2  YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=hdp2.0.6  YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=hdp2.1    YARN=false L4JOVR=false
	$(MAKE) build_inner HADOOP_VERSION=hdp2.2    YARN=false L4JOVR=false
	rm -rf ../target/hadoop
	mkdir ../target/hadoop
	cp README.txt ../target/hadoop
	cp target/* ../target/hadoop

