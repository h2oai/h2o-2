
default: build

build: clean
	mkdir classes
	mkdir target
	javac -source 1.6 -target 1.6 -sourcepath src -d classes src/*.java
	jar cmf manifest.txt target/H2OLauncher.jar -C classes .
	mkdir -p ../target/launcher
	mv target/H2OLauncher.jar ../target/launcher

clean:
	rm -fr classes
	rm -fr target

