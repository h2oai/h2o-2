
default: build

build:
	ant -f h2o-cookbook.xml

test:
	java -cp `cat ../target/classpath | sed 's/lib/..\/lib/g'`:../target/h2o.jar:out/test/test cookbook.CookbookRunner

clean:
	ant -f h2o-cookbook.xml clean

