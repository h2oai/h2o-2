
build:
	./build.sh build

test:
	./build.sh

# Set accordingly
JAVA_HEAP_SIZE=-Xmx2g
run:
	java $(JAVA_HEAP_SIZE) -jar target/h2o.jar

clean:
	./build.sh clean

