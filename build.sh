#! /bin/bash

# determine the correct separator of multiple paths
if [ `uname` = "Darwin" ]
then 
    UNAMECMD="uname"
else 
    UNAMECMD="uname -o"
fi

if [ `$UNAMECMD` = "Cygwin" ]
then
    SEP=";"
else
    SEP=":"
fi

# Setup script error handling
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

# ------------------------------------------------------------------------------
# basic build properties
# ------------------------------------------------------------------------------
# This is where the source files (java) and resources are relative to the path of this file
      SRC=src/main/java
  TESTSRC=src/test/java
RESOURCES=src/main/resources
# and this is where the jar contents is stored relative to this file again
JAR_ROOT=lib

# additional dependencies, relative to this file, but all dependencies should be
# inside the JAR_ROOT tree so that they are packed to the jar file properly
DEPENDENCIES="\
${JAR_ROOT}/jama/*\
${SEP}${JAR_ROOT}/apache/*\
${SEP}${JAR_ROOT}/junit/*\
${SEP}${JAR_ROOT}/gson/*\
${SEP}${JAR_ROOT}/json/*\
${SEP}${JAR_ROOT}/javassist.jar\
${SEP}${JAR_ROOT}/poi/*\
${SEP}${JAR_ROOT}/s3/*\
${SEP}${JAR_ROOT}/jets3t/*\
${SEP}${JAR_ROOT}/log4j/*\
${SEP}${JAR_ROOT}/joda/*\
${SEP}${JAR_ROOT}/tachyon/*\
${SEP}${JAR_ROOT}/jogamp/*"

DEFAULT_HADOOP_VERSION="cdh3"
OUTDIR="target"
JAR_FILE="${OUTDIR}/h2o.jar"
SRC_JAR_FILE="${OUTDIR}/h2o-sources.jar"
IMODEL_JAR_FILE="${OUTDIR}/h2o-model.jar"

JAVA=`which java`||echo 'Missing java, please install jdk'
JAVAC=`which javac`||echo 'Missing javac, please install jdk'
if [ "${LOGNAME}" = "jenkins" ]; then
    # This is really awful, but javadoc 6 is reliably hitting this failure in Linux.
    # Workaround this for now.
    #
    #    Standard Doclet version 1.6.0_45
    #    Building tree for all the packages and classes...
    #    java.lang.NullPointerException
    #            at com.sun.tools.javadoc.TypeMaker.getType(TypeMaker.java:67)
    #            at com.sun.tools.javadoc.TypeMaker.getType(TypeMaker.java:29)
    #            at com.sun.tools.javadoc.ParameterizedTypeImpl.superclassType(ParameterizedTypeImpl.java:62)
    #            at com.sun.tools.doclets.internal.toolkit.util.Util.findAllInterfaceTypes(Util.java:441)
    #            at com.sun.tools.doclets.internal.toolkit.util.Util.addAllInterfaceTypes(Util.java:472)
    #            ...
    #
    JAVADOC=/usr/lib/jvm/java-7-oracle/bin/javadoc
else
    JAVADOC=`which javadoc`||echo 'Missing javadoc, please install jdk'
fi

# need bootclasspath to point to jdk1.6 rt.jar bootstrap classes
# extdirs can also be passed as -extdirs 

JAVAC_ARGS="-g
    -source 1.6
    -target 1.6
    -J-Xms1024m 
    -J-Xmx1024m
    -XDignore.symbol.file
    -Xlint:all
    -Xlint:-deprecation
    -Xlint:-serial
    -Xlint:-rawtypes
    -Xlint:-path
    -Xlint:-unchecked "
JAR=`which jar`
ZIP=`which zip`
GIT=`which git || which false`
CLASSES="${OUTDIR}/classes"
VERSION_PROPERTIES="${CLASSES}/version.properties"

# Clean up also /tmp content (/tmp/h2o-temp-*, /tmp/File*tmp)
# Note: /tmp is specific for Linux.
WIPE_TMP=true

# Calculate MYNAME based on platform.
if [ ! -z "$LOGNAME" ]; then
    # Unix
    MYNAME=`echo ${LOGNAME} | sed 's/ /_/g'`
else
    if [ ! -z "$USERNAME" ]; then
        # Windows
        MYNAME=`echo ${USERNAME} | sed 's/ /_/g'`
    else
        MYNAME="UnknownLogin"
    fi
fi

# Load user-specific properties if they are exist.
# The properties can override the settings above.
LOCAL_PROPERTIES_FILE="./build.local.conf"
[ -f "$LOCAL_PROPERTIES_FILE" ] && source "$LOCAL_PROPERTIES_FILE"

# ------------------------------------------------------------------------------
# script  
# ------------------------------------------------------------------------------

function clean() {
    echo "cleaning..."
    rm -fr ${OUTDIR}
    rm -fr lib/javassist
    if [ "$WIPE_TMP" = "true" ]; then
        echo " - wiping tmp..."
        rm -fr /tmp/h2o-"${MYNAME}"/h2o-temp-*
    fi
    mkdir ${OUTDIR}
    mkdir ${CLASSES}
    rm -fr hadoop/target
}

function build_classes() {
    echo "building classes..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    echo $CLASSPATH > target/classpath
    "$JAVAC" ${JAVAC_ARGS} \
        -cp "${CLASSPATH}" \
        -sourcepath "$SRC" \
        -d "$CLASSES" \
        $SRC/water/*java \
        $SRC/water/*/*java \
        $SRC/jsr166y/*java \
        $TESTSRC/*/*java \
        $TESTSRC/*/*/*java

    # Copy Java resources
    # If you delete this line you take responsibility for logging and you have to make
    # sure that jests3 library will not output its logs into our log files !!!
    cp -r ${RESOURCES}/* "${CLASSES}"
}

function build_initializer() {
    echo "building initializer..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    pushd lib
    "$JAR" xf javassist.jar
    rm -rf META-INF
    popd
}

function build_jar() {
    echo "creating jar file... ${JAR_FILE}"
    # include all libraries
    cd ${JAR_ROOT}
    "$JAR" -cfm ../${JAR_FILE} ../manifest.txt `/usr/bin/find . -type f | grep -v 'sources.jar' | grep -v 'hadoop/mapr' | grep -v 'hdp1.3.2' | grep -v 'hdp2.0.6'`
    cd ..
    # include H2O classes
    "$JAR" uf ${JAR_FILE} -C "${CLASSES}"   .
    # Pick up R jars
    if [ -d r_pack_tmp ]; then
      "$JAR" uf ${JAR_FILE} -C r_pack_tmp  .
      rm -fr r_pack_tmp
    fi
    "$ZIP" -qd ${JAR_FILE} javassist.jar 
}

function build_src_jar() {
    echo "creating src jar file... ${SRC_JAR_FILE}"
    # include H2O source files
    "$JAR" cf ${SRC_JAR_FILE} -C "${SRC}" .
    "$JAR" uf ${SRC_JAR_FILE} -C "${TESTSRC}" .
}

function build_imodel_jar() {
    echo "creating imodel jar file... ${IMODEL_JAR_FILE}"
    "$JAR" cf ${IMODEL_JAR_FILE} -C "${CLASSES}" "water/genmodel" -C "${CLASSES}" "water/util/ModelUtils.class"
    [ -d "$CLASSES/resources/" ] || mkdir -p "$CLASSES/resources/"
    cp "${IMODEL_JAR_FILE}" "$CLASSES/resources/"
}

function build_samples() {
    echo "building samples..."
    mkdir -p h2o-samples/target/classes
    "$JAVAC" \
    	-cp ${JAR_FILE}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/* \
    	-d h2o-samples/target/classes \
    	-sourcepath h2o-samples/src/main/java \
    	h2o-samples/src/main/java/*/*java
}

function build_javadoc() {
    echo "creating javadoc files..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    mkdir -p target/logs
    "${JAVADOC}" -J-Xms256m -J-Xmx256m -overview ${SRC}/overview.html -classpath "${CLASSPATH}" -d "${OUTDIR}"/javadoc -sourcepath "${SRC}" -subpackages hex:water >& target/logs/javadoc_build.log
}

function junit() {
    echo "running JUnit tests..."
    "$JAVA" -Xmx1g -ea -cp ${JAR_FILE} water.Boot -mainClass water.JUnitRunner
}

if [ "$1" = "onlydoc" ]; then
    build_javadoc
    exit 0
fi
if [ "$1" = "noclean" ]; then
    shift
else
    clean
fi
if [ "$1" = "clean" ]; then exit 0; fi
build_classes
if [ "$1" = "compile" ]; then exit 0; fi
build_initializer
build_imodel_jar
build_jar
cp "target/h2o.jar" "h2o-perf/perf-target/"
build_src_jar
build_samples
if [ "$1" = "build" ]; then exit 0; fi
build_javadoc
if [ "$1" = "doc" ]; then exit 0; fi
junit
