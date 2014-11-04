== Introduction ==

This project contains shared parent POM file for H2O build infrastructure.

== Usage ==

When starting a new project, please use the already configured _parent pom_ to avoid duplication of the same information:

=== Maven ===
{{{
<parent>
  <groupId>ai.h2o</groupId>
  <artifactId>h2o-classic-parent</artifactId>
</parent>
}}}

=== Gradle ===
TODO

== Details ==

=== Sonatype ===
For more details please check the page: https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide.

==== Making snapshot ====
Put your Sonatype credentials into .m2/setting.xml file:
```
<servers>
   <server>
      <id>sonatype-nexus-snapshots</id>
      <username>your-jira-id</username>
      <password>your-jira-pwd</password>
    </server>
    <server>
      <id>sonatype-nexus-staging</id>
      <username>your-jira-id</username>
      <password>your-jira-pwd</password>
    </server>
  </servers>

```

Invoke deploy command:
```
mvn clean deploy
```

==== Making release ====

```
mvn clean release
```
