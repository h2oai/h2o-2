name := "h2o-scala"

version := "1.0"

organization := "0xdata.com"

scalaVersion := "2.10.3"

mainClass in Compile := Some("water.api.dsl.ShalalaRepl")

/**
 * Configure dependencies - compile and runtime.
 */
libraryDependencies += "commons-lang" % "commons-lang" % "2.4"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.1.1"

libraryDependencies += "com.google.guava" % "guava" % "12.0.1"

libraryDependencies += "com.google.code.gson" % "gson" % "2.2.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.8"

libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.6.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.6.2"

libraryDependencies += "org.javassist" % "javassist" % "3.16.1-GA"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.1.0"

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "scala-library" % v }

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "scala-compiler" % v }

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "jline" % v }

// Test dependencies
libraryDependencies += "org.specs2" %% "specs2" % "2.2.3" % "test"

//scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")


// Setup classpath to have access to h2o.jar
val h2oClasses = baseDirectory / "../target/classes/"
val h2oSources = baseDirectory / "../src/"

unmanagedClasspath in Compile += h2oClasses.value

unmanagedClasspath in Compile += h2oSources.value

unmanagedClasspath in Runtime += h2oClasses.value

// Setup run 
// - Fork in run
fork in run := true

connectInput in run := true

outputStrategy in run := Some(StdoutOutput)

//javaOptions in run += "-Xdebug"

//javaOptions in run += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

// - Change the base directory
baseDirectory in run := h2oClasses.value

