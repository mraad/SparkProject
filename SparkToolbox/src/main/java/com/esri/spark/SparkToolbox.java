package com.esri.spark;

import com.esri.arcgis.geodatabase.IEnumGPName;
import com.esri.arcgis.geodatabase.IGPName;
import com.esri.arcgis.geoprocessing.EnumGPName;
import com.esri.arcgis.geoprocessing.IEnumGPEnvironment;
import com.esri.arcgis.geoprocessing.IGPFunction;
import com.esri.arcgis.geoprocessing.IGPFunctionFactory;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.extn.ArcGISCategories;
import com.esri.arcgis.interop.extn.ArcGISExtension;
import com.esri.arcgis.system.IUID;
import com.esri.arcgis.system.UID;

import java.io.IOException;
import java.util.UUID;

/**
 */
@ArcGISExtension(categories = {ArcGISCategories.GPFunctionFactories})
public final class SparkToolbox implements IGPFunctionFactory
{
    private static final String NAME = SparkToolbox.class.getSimpleName();

    public IUID getCLSID() throws IOException, AutomationException
    {
        final UID uid = new UID();
        uid.setValue("{" + UUID.nameUUIDFromBytes(this.getClass().getName().getBytes()) + "}");
        return uid;
    }

    public String getName() throws IOException, AutomationException
    {
        return NAME;
    }

    public String getAlias() throws IOException, AutomationException
    {
        return NAME;
    }

    public IGPFunction getFunction(final String s) throws IOException, AutomationException
    {
        if (SparkTool.NAME.equalsIgnoreCase(s))
        {
            return new SparkTool();
        }
        if (HexTool.NAME.equalsIgnoreCase(s))
        {
            return new HexTool();
        }
        return null;
    }

    public IGPName getFunctionName(final String s) throws IOException, AutomationException
    {
        if (SparkTool.NAME.equalsIgnoreCase(s))
        {
            return SparkTool.getFunctionName(this);
        }
        if (HexTool.NAME.equalsIgnoreCase(s))
        {
            return HexTool.getFunctionName(this);
        }
        return null;
    }

    public IEnumGPName getFunctionNames() throws IOException, AutomationException
    {
        final EnumGPName nameArray = new EnumGPName();
        nameArray.add(SparkTool.getFunctionName(this));
        nameArray.add(HexTool.getFunctionName(this));
        return nameArray;
    }

    public IEnumGPEnvironment getFunctionEnvironments() throws IOException, AutomationException
    {
        return null;
    }
}