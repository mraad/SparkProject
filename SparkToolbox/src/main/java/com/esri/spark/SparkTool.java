package com.esri.spark;

import com.esri.SparkApp;
import com.esri.arcgis.datasourcesfile.DEFile;
import com.esri.arcgis.geodatabase.Field;
import com.esri.arcgis.geodatabase.Fields;
import com.esri.arcgis.geodatabase.ICursor;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPName;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.IRowBuffer;
import com.esri.arcgis.geodatabase.Table;
import com.esri.arcgis.geodatabase.esriFieldType;
import com.esri.arcgis.geoprocessing.GPFunctionName;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.IGPFunctionFactory;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import com.esri.arcgis.system.ITrackCancel;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 */
public final class SparkTool extends AbstractTool
{
    public static final String NAME = SparkTool.class.getSimpleName();

    private final static String ORIGID = "OrigID";
    private final static String TOTAL = "Total";

    static IGPName getFunctionName(final IGPFunctionFactory functionFactory) throws IOException
    {
        final GPFunctionName functionName = new GPFunctionName();
        functionName.setCategory(NAME);
        functionName.setDescription(NAME);
        functionName.setDisplayName(NAME);
        functionName.setName(NAME);
        functionName.setFactoryByRef(functionFactory);
        return functionName;
    }

    @Override
    protected void doExecute(
            final IArray parameters,
            final ITrackCancel trackCancel,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages
    ) throws Exception
    {
        final IGPValue sparkValue = gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue shapeValue = gpUtilities.unpackGPValue(parameters.getElement(1));
        final IGPValue hadoopValue = gpUtilities.unpackGPValue(parameters.getElement(2));
        final IGPValue tableValue = gpUtilities.unpackGPValue(parameters.getElement(3));

        final File file = new File(shapeValue.getAsText());

        final Properties properties = readSparkProperties(sparkValue);
        final SparkApp sparkApp = new SparkApp();
        final List<Tuple2<Integer, Integer>> list = sparkApp.run(properties, file.toURI().toURL(), hadoopValue.getAsText());
        createTable(list, tableValue, messages, environmentManager);
    }

    private void createTable(
            final List<Tuple2<Integer, Integer>> list,
            final IGPValue tableValue,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws IOException
    {
        int count = 0;
        final String tableName = tableValue.getAsText();
        if (gpUtilities.exists(tableValue))
        {
            messages.addMessage(String.format("%s already exists, overwriting it...", tableName));
            gpUtilities.delete(tableValue);
        }
        final IFields fields = createFields();
        final Table table = createTable(tableName, fields, messages, environmentManager);
        final int m_indexFID = table.findField(ORIGID);
        final int m_indexTotal = table.findField(TOTAL);

        final IRowBuffer m_rowBuffer = table.createRowBuffer();
        final ICursor m_insert = table.insert(true);
        try
        {
            for (Tuple2<Integer, Integer> tuple2 : list)
            {
                m_rowBuffer.setValue(m_indexFID, tuple2._1());
                m_rowBuffer.setValue(m_indexTotal, tuple2._2());
                m_insert.insertRow(m_rowBuffer);
                count++;
            }
        }
        finally
        {
            m_insert.flush();
        }
        messages.addMessage(String.format("Created %d rows(s)", count));
    }

    private IFields createFields() throws IOException
    {
        final Fields fields = new Fields();

        addFieldInt(fields, ORIGID);
        addFieldInt(fields, TOTAL);

        return fields;
    }

    private void addFieldInt(
            final Fields fields,
            final String name) throws IOException
    {
        final Field field = new Field();
        field.setName(name);
        field.setType(esriFieldType.esriFieldTypeInteger);
        fields.addField(field);
    }

    private Properties readSparkProperties(final IGPValue gpValue) throws IOException
    {
        final Properties properties = new Properties();
        final DEFile deFile = (DEFile) gpValue;
        final FileInputStream fileInputStream = new FileInputStream(deFile.getCatalogPath());
        try
        {
            properties.load(fileInputStream);
        }
        finally
        {
            fileInputStream.close();
        }
        return properties;
    }

    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final IArray parameters = new Array();

        addParamFile(parameters, "Spark properties", "in_spark", "C:\\temp\\spark.properties");
        addParamFeatureClassIn(parameters, "Polygon shapefile", "in_shapefile");
        addParamString(parameters, "Hadoop file", "in_hadoop", "/user/cloudera/zip.txt");
        addParamTable(parameters, "Output table", "out_table", "C:\\temp\\table.dbf");

        return parameters;
    }

    @Override
    public String getName() throws IOException, AutomationException
    {
        return NAME;
    }

    @Override
    public String getDisplayName() throws IOException, AutomationException
    {
        return NAME;
    }
}
