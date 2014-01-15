package com.esri.spark;

import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.Field;
import com.esri.arcgis.geodatabase.Fields;
import com.esri.arcgis.geodatabase.GeometryDef;
import com.esri.arcgis.geodatabase.ICursor;
import com.esri.arcgis.geodatabase.IFeatureBuffer;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPName;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.esriFieldType;
import com.esri.arcgis.geometry.GeometryEnvironment;
import com.esri.arcgis.geometry.IEnvelope;
import com.esri.arcgis.geometry.esriGeometryType;
import com.esri.arcgis.geoprocessing.GPFunctionName;
import com.esri.arcgis.geoprocessing.IGPDouble;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.IGPFunctionFactory;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.Cleaner;
import com.esri.arcgis.system.IArray;

import java.io.IOException;

/**
 */
public final class HexTool extends AbstractTool
{
    public static final String NAME = HexTool.class.getSimpleName();
    public static final String SHAPE = "Shape";

    private GeometryEnvironment m_geometryEnv;
    private IFeatureBuffer m_featureBuffer;
    private ICursor m_insert;
    private int m_count;

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
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager
    ) throws Exception
    {
        final IGPDouble gridValue = (IGPDouble) gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue outValue = gpUtilities.unpackGPValue(parameters.getElement(1));

        final double size = gridValue.IGPDouble_getValue();
        final Hex hex = new Hex(size);

        m_count = 0;
        m_geometryEnv = new GeometryEnvironment();
        try
        {
            final String featureClassAsText = outValue.getAsText();
            if (gpUtilities.exists(outValue))
            {
                messages.addMessage(String.format("%s already exists, overwriting it...", featureClassAsText));
                gpUtilities.delete(outValue);
            }
            final IFields fields = createFields();
            final FeatureClass featureClass = createFeatureClass(featureClassAsText, fields, SHAPE, environmentManager, messages);
            try
            {
                m_featureBuffer = featureClass.createFeatureBuffer();
                m_insert = featureClass.insert(true);
                try
                {
                    final IEnvelope extent = gpUtilities.getActiveView().getExtent();
                    final double xMin = extent.getXMin();
                    final double xMax = extent.getXMax() + size;
                    final double yMin = extent.getYMin();
                    final double yMax = extent.getYMax() + size;
                    int row = 0;
                    for (double y = yMin; y <= yMax; y += hex.v)
                    {
                        final double ofs = (row++ & 1) == 0 ? 0.0 : hex.w2;
                        for (double x = xMin + ofs; x <= xMax; x += hex.w)
                        {
                            m_featureBuffer.setShapeByRef(hex.toPolygon(m_geometryEnv, x, y));
                            m_insert.insertRow(m_featureBuffer);
                            m_count++;
                        }
                    }
                }
                finally
                {
                    m_insert.flush();
                }
            }
            finally
            {
                Cleaner.release(featureClass);
            }
        }
        finally
        {
            m_geometryEnv.release();
        }
        messages.addMessage(String.format("Created %d feature(s)", m_count));
    }

    private IFields createFields() throws IOException
    {
        final Fields fields = new Fields();

        addFieldShape(fields);

        return fields;
    }

    private void addFieldShape(final Fields fields) throws IOException
    {
        final GeometryDef geometryDef = new GeometryDef();
        geometryDef.setGeometryType(esriGeometryType.esriGeometryPolygon);
        geometryDef.setHasM(false);
        geometryDef.setHasZ(false);
        geometryDef.setSpatialReferenceByRef(gpUtilities.getMap().getSpatialReference());

        final Field field = new Field();
        field.setName(SHAPE);
        field.setType(esriFieldType.esriFieldTypeGeometry);
        field.setGeometryDefByRef(geometryDef);
        fields.addField(field);
    }

    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final IArray parameters = new Array();

        final double size = gpUtilities.getActiveView().getExtent().getWidth() / 128.0;
        addParamDouble(parameters, "Hex size in map units", "in_hex_size", Math.round(size * 1000.0) / 1000.0);
        addParamFeatureClassOut(parameters, "Output feature class", "out_output", "C:\\temp\\hex.shp");

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
