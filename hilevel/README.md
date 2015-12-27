Proof-of-concept application using DOT notation to specify DAG
=======

This directory contains an Apache Apex project demonstrating how to simplify
the code needed to create the application DAG by using the common DOT
notation to specify the DAG.

To build and launch it you'll need to perform a couple of preliminary steps:

1. Download version 0.8.6 of the JPGD parser from
   [JPGD](http://www.alexander-merz.com/graphviz/) and unpack it.

2. Install the jar `com.alexmerz.graphviz.jar` from it into your local
   maven repository with the following coordinates:

       groupId = jpgd
       artifactId = jpgd
       version = 0.8.6

   Some ways of doing this are discussed
   [here](https://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html),
   [here](http://maven.apache.org/plugins/maven-install-plugin/usage.html), and
   [here](https://forums.netbeans.org/topic22907.html). After doing this, your
   local repository should have something like this under
   `~/.m2/repository/jpgd/jpgd/0.8.6/`

       -rw-rw-r-- 1 xxx xxx 66284 Dec 26 18:35 jpgd-0.8.6.jar
       -rw-rw-r-- 1 xxx xxx   452 Dec 26 19:10 jpgd-0.8.6.pom
       -rw-rw-r-- 1 xxx xxx   143 Dec 26 19:25 _maven.repositories
       -rw-rw-r-- 1 xxx xxx   167 Dec 26 19:10 _remote.repositories

3. Now you can build the project as usual with `mvn clean package -DskipTests`.
   Just check that `jpgd-0.8.6.jar` is present in the package:

       jar tvf target/*.apa | grep jpgd

4. The `.apa` package can be uploaded and launched as usual.

   
