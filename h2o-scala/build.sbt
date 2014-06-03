name := "h2o-scala"

version := "2.5-SNAPSHOT"

organization := "ai.h2o"

scalaVersion := "2.10.3"

mainClass in Compile := Some("water.api.dsl.ShalalaRepl")

/* Add sonatype repo to get H2O */
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

/** Add cloudera repo */
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos"

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies
//resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
//                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")

/**
 * Configure dependencies - compile and runtime.
 */
libraryDependencies += "ai.h2o" % "h2o-core" % "2.5-SNAPSHOT"

libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "latest.integration"

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "scala-library" % v }

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "scala-compiler" % v }

libraryDependencies <+= scalaVersion { v => "org.scala-lang" % "jline" % v }


// Test dependencies
libraryDependencies += "org.specs2" %% "specs2" % "2.2.3" % "test"

//scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions in Test ++= Seq("-Yrangepos")

// Setup run
// - Fork in run
fork in run := true

connectInput in run := true

outputStrategy in run := Some(StdoutOutput)

javaOptions in run ++= Seq("-Xmx4g", "-Xms4g")

// EclipseKeys.withSource := true
//javaOptions in run += "-Xdebug"

//javaOptions in run += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

lazy val runExamples = taskKey[Unit]("Run examples")

fullRunTask(runExamples, Runtime, "water.api.dsl.examples.Examples")

fork in runExamples := true

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

/** Publish to sonatype */
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

/** Publish as Maven artifacts */
publishMavenStyle := true

/* Do not publish test artifacts */
publishArtifact in Test := false

pomExtra := (
  <url>http://h2o.ai/</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:https://github.com/0xdata/h2o.git</connection>
    <developerConnection>scm:git:git@github.com:0xdata/h2o.git</developerConnection>
    <url>https://github.com/0xdata/h2o</url>
  </scm>
)

