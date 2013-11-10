#!/bin/bash

sbt package
cp ./target/scala-2.10/h2o-scala_2.10-1.0.jar ../lib/h2o-scala/h2o-scala.jar

( cd ../; ./build.sh build ; java -jar target/h2o.jar -scala_repl )

