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

Adjust `core-site.xml` the point to your Hadoop file system:

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

At the end of the execution, you should see a list of tuples, where the first item in the tuple is the polygon identifier and the second item is the number of zipcodes that are covered by that polygon.

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

The Spark driver application starts by creating a [JavaSparkContext](http://spark.incubator.apache.org/docs/latest/api/core/index.html#org.apache.spark.api.java.JavaSparkContext)
that points to a spark master and to a list of dependent jars. By default, all these jars are copied into to the `target` folder when maven packages
the application.
The shapefile that contains the binning polygons is opened and each read feature is stored in a spatial index.
That spatial index is broadcasted out to the nodes as a [shared variable](https://spark.incubator.apache.org/docs/0.8.1/scala-programming-guide.html#shared-variables).
An [RDD](https://spark.incubator.apache.org/docs/0.8.1/scala-programming-guide.html#resilient-distributed-datasets-rdds) is defined by referencing a Hadoop path.
That path contains a set of records in text format.  Each record contains 3 fields separated by a `,` and each record is terminated by `\n`.
The first field is a record identifier, the second field is a longitude value and the third field is a latitude value.
A `flatMap` function is applied to the RDD to tokenize each record and parse to double the latitude and longitude tokens.
Using the broadcasted spatial index, the latitude and longitude values are used to query what polygon (if any) covers that location.
If a polygon is found, then a tuple with the polygon id and a value of 1 is the "map" value of the line.
Note that a flatMap was used rather than a map as flatMap can return an empty list in case of a tokenization or parsing failure.
I should have used an Accumulator to tally the number of bad records.
Onto the `flatMap` result is applied a `reduceByKey` function that simply sums the `1s`.
The result of the `reduceByKey` is `collected` into a list of tuple where the first element is the polygon id and the second element is the aggregated sum.

