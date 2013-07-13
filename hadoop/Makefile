
build: clean
	mkdir classes
	javac -sourcepath src/main/java -classpath lib/h2o.jar:lib/hadoop-0.20.2-core.jar -d classes src/main/java/*.java
	mkdir target
	jar cf target/H2ODriver.jar -C classes .

clean:
	rm -fr classes target

