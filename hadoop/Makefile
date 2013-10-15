#
# For each supported HADOOP_VERSION, add _JAR_DIR and _JARS below,
# and add a line to the build target.
#

cdh3_JAR_DIR=../lib/hadoop/cdh3
cdh3_JARS=$(cdh3_JAR_DIR)/hadoop-core-0.20.2-cdh3u6.jar

cdh4_JAR_DIR=../lib/hadoop/cdh4
cdh4_JARS=$(cdh4_JAR_DIR)/hadoop-common.jar${SEP}${cdh4_JAR_DIR}/hadoop-annotations-2.0.0-cdh4.2.0.jar${SEP}$(cdh4_JAR_DIR)/hadoop-mapreduce-client-core-2.0.0-cdh4.2.0.jar

cdh4_yarn_JAR_DIR=../lib/hadoop/cdh4_yarn
cdh4_yarn_JARS=$(cdh4_yarn_JAR_DIR)/common/hadoop-common-2.0.0-cdh4.3.0.jar${SEP}${cdh4_yarn_JAR_DIR}/common/lib/hadoop-annotations-2.0.0-cdh4.3.0.jar${SEP}$(cdh4_yarn_JAR_DIR)/mapreduce2/hadoop-mapreduce-client-core-2.0.0-cdh4.3.0.jar

mapr2.1.3_JAR_DIR=../lib/hadoop/mapr2.1.3
mapr2.1.3_JARS=$(mapr2.1.3_JAR_DIR)/hadoop-0.20.2-dev-core.jar

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
	@echo "}" >> $(F)
	mkdir classes/$(HADOOP_VERSION)
	javac -Xlint:deprecation -source 1.6 -target 1.6 -sourcepath src/main/java -classpath "$(COMMON_JARS)${SEP}$(HADOOP_JARS)" -d classes/$(HADOOP_VERSION) src/main/java/water/hadoop/*.java
	jar cf target/h2odriver_$(HADOOP_VERSION).jar -C classes/$(HADOOP_VERSION) .

clean:
	rm -fr classes target

build: clean
	mkdir classes
	mkdir target
	$(MAKE) build_inner HADOOP_VERSION=mapr2.1.3 YARN=false
	$(MAKE) build_inner HADOOP_VERSION=cdh3      YARN=false
	$(MAKE) build_inner HADOOP_VERSION=cdh4      YARN=false
	$(MAKE) build_inner HADOOP_VERSION=cdh4_yarn YARN=true
	rm -rf ../target/hadoop
	mkdir ../target/hadoop
	cp README.txt ../target/hadoop
	cp target/* ../target/hadoop

