
build:
	./build.sh build
	make -C hadoop

test:
	./build.sh

# Set accordingly
JAVA_HEAP_SIZE=-Xmx2g
run:
	java $(JAVA_HEAP_SIZE) -jar target/h2o.jar

clean:
	./build.sh clean
	make -C hadoop clean

