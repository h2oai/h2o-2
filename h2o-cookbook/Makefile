
default: build

build:
	ant -f h2o-cookbook.xml
	jar cf h2o-cookbook.jar -C out/test/test .

test:
	java -cp `cat ../target/classpath | sed 's/lib/..\/lib/g'`:../target/h2o.jar:h2o-cookbook.jar cookbook.CookbookRunner

run_grouped_average:
	java -cp `cat ../target/classpath | sed 's/lib/..\/lib/g'`:../target/h2o.jar:h2o-cookbook.jar cookbook.groupedaverage.GroupedAverageWithHttpRunner

clean:
	ant -f h2o-cookbook.xml clean
	rm -f h2o-cookbook.jar

