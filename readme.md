Spring Data Cassandra
=====================

This is a Genuine Spring Data subproject for Cassandra that uses the binary CQL3 protocol via
the official DataStax 1.x Java driver (https://github.com/datastax/java-driver) for Cassandra 1.2.

Supports native CQL3 queries in Spring Repositories.

Versions
--------

 - CQL 3.0

 - Cassandra 1.2

 - Datastax Java Driver 1.0.4-dse

 - JRE >= 1.6

CQL
--------

Fully supported CQL 3.0 version. Specification is available [here] (http://www.datastax.com/documentation/cql/3.0/pdf/cql30.pdf)


Building
--------
This is a standard Maven multimodule project.  Just issue the command `mvn clean install` from the repo root.

Snapshot builds are available [here](https://oss.sonatype.org/index.html#nexus-search;quick~org.springdata)

Using
-------

Add snapshot repository to the pom.xml

```

<repositories>
  <repository>
    <id>sonatype-nexus-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  </repository>
</repositories>

```

Add dependencies to pom.xml

```

<dependency>
  <groupId>org.springdata</groupId>
  <artifactId>spring-cassandra-cql</artifactId>
  <version>1.2.0.BUILD-SNAPSHOT</version>
</dependency>

<dependency>
  <groupId>org.springdata</groupId>
  <artifactId>spring-data-cassandra</artifactId>
  <version>1.2.0.BUILD-SNAPSHOT</version>
</dependency>

```


Continuous integration
--------
https://travis-ci.org/SpringData/spring-data-cassandra


Discussion forum
--------
http://sdc.1003000.n3.nabble.com/


Contribution
--------

A few steps to make this process easy for us:

 - Please use [eclipse-formatting.xml] (eclipse-formatting.xml) or similar formatting for Idea with auto-save option.
 - Post a new feature/bug on discussion [forum](http://sdc.1003000.n3.nabble.com/).
 - Create a new issue in https://github.com/SpringData/spring-data-cassandra/issues/ and refer this issue in the push request.
 - Thank you.

