package com.esri;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.util.NullProgressListener;
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureVisitor;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SparkApp
        implements Serializable
{

    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_HOME = "spark.home";

    public static void main(final String[] args) throws IOException
    {
        final SparkApp sparkApp = new SparkApp();
        final File file = new File("/tmp/hex.shp");
        final String shapefile = file.toURI().toURL().toString();
        final Properties properties = new Properties();
        final List<Tuple2<Integer, Integer>> list = sparkApp.run(
                properties,
                shapefile,
                "/user/cloudera/zip.txt");
        System.out.println(list);
    }

    public List<Tuple2<Integer, Integer>> run(
            final Properties properties,
            final String polygonFile,
            final String hadoopFile) throws IOException
    {
        final List<Tuple2<Integer, Integer>> collect;
        final JavaSparkContext sc = getJavaSparkContext(properties);
        try
        {
            sc.addFile(polygonFile);
            sc.addFile(polygonFile.replace(".shp", ".shx"));
            sc.addFile(polygonFile.replace(".shp", ".dbf"));
            final String polygonName = toPolygonName(polygonFile);
            final Broadcast<SpatialIndex> spatialIndexBroadcast = broadcastSpatialIndex(sc, polygonName);
            collect = sc.hadoopFile(
                    hadoopFile,
                    TextInputFormat.class,
                    LongWritable.class,
                    Text.class).
                    flatMap(new SpatialMapFunction(spatialIndexBroadcast)).
                    reduceByKey(new SpatialReduceFunction()).
                    collect();
        }
        finally
        {
            sc.stop();
        }
        return collect;
    }

    private String toPolygonName(final String polygonFile)
    {
        final String[] tokens = polygonFile.split("/");
        return tokens[tokens.length - 1];
    }

    private Broadcast<SpatialIndex> broadcastSpatialIndex(
            final JavaSparkContext sc,
            final String filename) throws IOException
    {
        final SpatialIndex spatialIndex = new STRtree();

        final File file = new File(SparkFiles.get(filename));
        final Map<String, Serializable> params = new HashMap<String, Serializable>();
        params.put("url", file.toURI().toURL());
        final DataStore dataStore = DataStoreFinder.getDataStore(params);
        try
        {
            final MutableInt mutableInt = new MutableInt(0);
            final String[] typeNames = dataStore.getTypeNames();
            final SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeNames[0]);
            final String geomName = featureSource.getSchema().getGeometryDescriptor().getLocalName();
            final SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            featureCollection.accepts(new FeatureVisitor()
            {
                public void visit(final Feature feature)
                {
                    final MultiPolygon multiPolygon = (MultiPolygon) feature.getProperty(geomName).getValue();
                    final Geometry geometry = multiPolygon.getGeometryN(0);
                    final PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);
                    spatialIndex.insert(geometry.getEnvelopeInternal(), new PreparedFeature(preparedGeometry, mutableInt.intValue()));
                    mutableInt.increment();
                }
            }, new NullProgressListener());
        }
        finally
        {
            dataStore.dispose();
        }
        return sc.broadcast(spatialIndex);
    }

    protected JavaSparkContext getJavaSparkContext(final Properties properties) throws MalformedURLException
    {
        final String libs = properties.getProperty("spark.libs", "target") + File.separator;
        final String[] jars = new String[]{
                libs + "SparkApp-1.0.jar",
                libs + "commons-pool-1.5.4.jar",
                libs + "gt-api-10-SNAPSHOT.jar",
                libs + "gt-data-10-SNAPSHOT.jar",
                libs + "gt-main-10-SNAPSHOT.jar",
                libs + "gt-metadata-10-SNAPSHOT.jar",
                libs + "gt-opengis-10-SNAPSHOT.jar",
                libs + "gt-referencing-10-SNAPSHOT.jar",
                libs + "gt-shapefile-10-SNAPSHOT.jar",
                libs + "jai_core-1.1.3.jar",
                libs + "jdom-1.0.jar",
                libs + "jgridshift-1.0.jar",
                libs + "jsr-275-1.0-beta-2.jar",
                libs + "jts-1.13.jar",
                libs + "vecmath-1.3.2.jar"
        };
        return new JavaSparkContext(
                properties.getProperty(SPARK_MASTER, "spark://cloudera.localdomain:7077"),
                this.getClass().getSimpleName(),
                properties.getProperty(SPARK_HOME, "/mnt/hgfs/spark-0.8.1-incubating-bin-cdh4"),
                jars);
    }

}
