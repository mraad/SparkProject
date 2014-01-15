SparkApp
========

Spark driver to spatially bin point data in an RDD using map reduce functions

## Data Setup

The polygon bins in this sample run are in the form of a honeycomb layer in the hex.zip file.
Unzip the content into the /tmp folder.

```
$ cp data/hex.zip /tmp
$ cd /tmp
$ unzip hex.zip
```

Unzip the content of the zip.zip to get the list of all the zip codes locations in the US, and place that content into HDFS.

```
$ cp data/zip.zip /tmp
$ cd /tmp
$ unzip zip.zip
$ hadoop fs -put zip.txt zip.txt
```

## Compile and run

Adjust _core-site.xml_ the point to your Hadoop file system:

```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://cloudera.localdomain:8020</value>
    </property>
</configuration>
```

Compile and install the application jar into you local maven repo:

```
$ mvn clean install
```

Run as a standalone application:

```
$ mvn exec:java
```

At the end of the execution, you should see a list of tuples, where the first item in the tuple is the polygon identifier and the second item is the number of zipcodes that covered by that polygon.

```
....
 INFO BlockManagerMaster stopped
 INFO Successfully stopped SparkContext
[(280,651), (284,38), (232,90), (302,23), (260,2933), (258,4685), (236,132), (300,23), (238,119), (282,3027), (278,13), (237,300), (259,12571), (301,140), (283,2355), (281,1032), (257,2223), (277,2)]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
...
```

## Application Flow/Description
