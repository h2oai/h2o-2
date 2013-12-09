name := "h2o-scala"

version := "1.0"

organization := "0xdata.com"

scalaVersion := "2.10.3"

/**
 * Configure dependencies.
 */
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.6.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "com.google.code.gson" % "gson" % "2.2.2"

libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.8"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.1.0"

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "scala-compiler" % v }

// Test dependencies
libraryDependencies += "org.specs2" %% "specs2" % "2.2.3" % "test"

//scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")

// Setup classpath
(unmanagedBase in Compile) := baseDirectory.value / "../target/"

//(unmanagedClasspath in Runtime) := (baseDirectory) map { bd => Attributed.blank(bd / "../target/classes/") }
(unmanagedClasspath in Runtime) := Seq( Attributed.blank( new File( "../target/classes/")), Attributed.blank( new File( "../lib/")))
