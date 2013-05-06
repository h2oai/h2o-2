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
DEPENDENCIES="${JAR_ROOT}/jama/*${SEP}${JAR_ROOT}/apache/*${SEP}${JAR_ROOT}/junit/*${SEP}${JAR_ROOT}/gson/*${SEP}${JAR_ROOT}/javassist.jar${SEP}${JAR_ROOT}/poi/*${SEP}${JAR_ROOT}/trove/*${SEP}${JAR_ROOT}/s3/*${SEP}${JAR_ROOT}/jets3t/*"

DEFAULT_HADOOP_VERSION="cdh3"
OUTDIR="target"
JAR_FILE="${OUTDIR}/h2o.jar"

JAVA=`which java`||echo 'Missing java, please install jdk'
JAVAC=`which javac`||echo 'Missing javac, please install jdk'

# need bootclasspath to point to jdk1.6 rt.jar bootstrap classes
# extdirs can also be passed as -extdirs 

JAVAC_ARGS="-g
    -source 1.6
    -target 1.6
    -XDignore.symbol.file
    -Xlint:all
    -Xlint:-deprecation
    -Xlint:-serial
    -Xlint:-rawtypes
    -Xlint:-unchecked "
JAR=`which jar`
ZIP=`which zip`
GIT=`which git || which false`
CLASSES="${OUTDIR}/classes"
VERSION_PROPERTIES="${CLASSES}/version.properties"

# Clean up also /tmp content (/tmp/h2o-temp-*, /tmp/File*tmp)
# Note: /tmp is specific for Linux.
WIPE_TMP=true

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
    if [ "$WIPE_TMP" = "true" ]; then
        echo " - wiping tmp..."
        rm -fr /tmp/h2o-temp-*
        rm -fr /tmp/File*tmp
    fi
    mkdir ${OUTDIR}
    mkdir ${CLASSES}
}

function build_classes() {
    echo "building classes..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    "$JAVAC" ${JAVAC_ARGS} \
        -cp "${CLASSPATH}" \
        -sourcepath "$SRC" \
        -d "$CLASSES" \
        $SRC/water/*java \
        $SRC/water/*/*java \
        $SRC/jsr166y/*java \
        $TESTSRC/*/*java \
        $TESTSRC/*/*/*java

    cp -r ${RESOURCES}/* "${CLASSES}"
cat >> "$VERSION_PROPERTIES" <<EOF
h2o.git.version=$($GIT rev-parse HEAD 2>/dev/null )
h2o.git.branch=$($GIT rev-parse --abbrev-ref HEAD 2>/dev/null )
EOF
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
    JAR_TIME=`date "+%H.%M.%S-%m%d%y"`
    echo "creating jar file... ${JAR_FILE}"
    # include all libraries
    cd ${JAR_ROOT}
    "$JAR" -cfm ../${JAR_FILE} ../manifest.txt `/usr/bin/find . -type f -not -name "*-sources.jar"`
    cd ..
    # include H2O classes
    "$JAR" uf ${JAR_FILE} -C "${CLASSES}"   .
    "$ZIP" -qd ${JAR_FILE} javassist.jar 
    echo "copying jar file... ${JAR_FILE} to ${OUTDIR}/h2o-${JAR_TIME}.jar"
    cp ${JAR_FILE} ${OUTDIR}/h2o-${JAR_TIME}.jar
}

function junit() {
    echo "running JUnit tests..."
    "$JAVA" -ea -cp ${JAR_FILE} water.Boot -mainClass water.JUnitRunner
}

clean
if [ "$1" = "clean" ]; then exit 0; fi
build_classes
if [ "$1" = "compile" ]; then exit 0; fi
build_initializer
build_jar
if [ "$1" = "build" ]; then exit 0; fi
junit
