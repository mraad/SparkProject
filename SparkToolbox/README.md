SparkToolbox
============

ArcGIS Desktop Geoprocessing Extension to invoke [Spark](http://spark.incubator.apache.org) jobs.

This toolbox contains 2 tools; HexTool and SparkTool.

## HexTool
HexTool is a geoprocessing tool that generates a polygon features class where each polygon is shaped as [hexagon](http://en.wikipedia.org/wiki/Hexagon).
When invoked, the tool prompts the user for the hexagon width in map units and based on the map viewing extent.

![HexTool](https://dl.dropboxusercontent.com/u/2193160/HexTool.png "HexTool")

It generates a honeycomb style set of polygons that fills the map.

![HexToolResult](https://dl.dropboxusercontent.com/u/2193160/HexToolRes.png "HexToolResult")

## SparkTool
SparkTool is yet another geoprocessing tool and is the Spark driver to execute the SparkApp on the cluster.
When invoked, it prompts the use to supply:

* the location of the spark cluster
* the feature class that contains the bins
* the HDFS location of the point data to bin
* The output table name

![SparkTool](https://dl.dropboxusercontent.com/u/2193160/SparkTool.png "SparkTool")

The `spark.properties` files contains the location of the dependent jars and the location of the spark home folder and the location of the spark master.

```
spark.master=spark://cloudera.localdomain:7077
spark.home=/mnt/hgfs/spark-0.8.1-incubating-bin-cdh4
spark.libs=C:\\Program Files (x86)\\ArcGIS\\Desktop10.1\\java\\lib\\ext\\libs
```

The resulting table has two columns.
The first column is the bin polygon identifier and the second column is the total number of HDFS data points that are covered by the bin area.
The output table is joined to the hex features class and symbolized:

![SparkToolJoin](https://dl.dropboxusercontent.com/u/2193160/SparkToolJoin.png "SparkToolJoin")

![SparkToolSymbol](https://dl.dropboxusercontent.com/u/2193160/SparkToolSymbol.png "SparkToolSymbol")

To produce:

![SparkToolHex](https://dl.dropboxusercontent.com/u/2193160/SparkToolHex.png "SparkToolHex")

From:

![SparkToolZipCodes](https://dl.dropboxusercontent.com/u/2193160/SparkToolZipCodes.png "SparkToolZipCodes")
