
We embed gson 2.3 into h2o and change the package name to avoid colliding with other software.
In particular, MapR 3.x has an ancient version of gson it includes in the classpath.


Note:  Before trying the steps below, I tried using 'jarjar' and didn't get it to work.


Steps:

Get gson sources into a fresh directory.
mkdir Gson23
cd Gson23
mkdir -p src/main/java
jar .../gson-2.3-sources.jar
mv com src/main/java
Import from sources into IDEA.
Use IDEA refactor 'move' step to move com.google.gson to dontweave.gson.
Project settings --> add Artifact jar file.
Build jar (make sure target is 1.6 language level).

cp -p out/artifacts/main_jar/gson-2.3.jar ~/0xdata/ws/h2o/lib/gson
cd to h2o directory
sed -i.sedbak 's/com.google.gson/dontweave.gson/g' `cat list_of_files`
rm `find . -name '*sedbak'`
make

