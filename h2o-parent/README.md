== Introduction ==

This project contains shared parent POM file for H2O build infrastructure.

== Usage ==

When starting a new project, please use the already configured _parent pom_ to avoid duplication of the same information:

=== Maven ===
{{{
<parent>
  <groupId>ai.h2o</groupId>
  <artifactId>h2o-parent</artifactId>
</parent>
}}}

=== Gradle ===
TODO

== Details ==

=== Sonatype ===
For more details please check the page: https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide.

==== Making snapshot ====

==== Making release ====
