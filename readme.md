Cassandrion
=====================

This is a Spring Data subproject for Cassandra that uses the binary CQL3 protocol via
the official DataStax 1.x Java driver (https://github.com/datastax/java-driver) for Cassandra 1.2.

Supports native CQL3 queries in Spring Repositories.

Versions
--------

 - CQL 3.0

 - Cassandra 1.2

 - Datastax Java Driver 1.0.4-dse


CQL
--------

Fully supported CQL 3.0 version. Specification is here http://www.datastax.com/documentation/cql/3.0/pdf/cql30.pdf


Cloning
-------
When cloning this repo, it's a good idea to also clone two others from Spring Data that this project depends on.  Assuming your current working directory is the root of this repository, issue the following commands:

	cd ..
	git clone https://github.com/spring-projects/spring-data-build.git
	git clone https://github.com/spring-projects/spring-data-commons.git


Building
--------
This is a standard Maven multimodule project.  Just issue the command `mvn clean install` from the repo root.


Continuous integration
--------
https://travis-ci.org/shvid/spring-data-cassandra
