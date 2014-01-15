package com.esri.spark;

import com.esri.arcgis.datasourcesGDB.FileGDBWorkspaceFactory;
import com.esri.arcgis.datasourcesfile.DEFile;
import com.esri.arcgis.datasourcesfile.DEFileType;
import com.esri.arcgis.datasourcesfile.DEFolder;
import com.esri.arcgis.datasourcesfile.DEFolderType;
import com.esri.arcgis.datasourcesfile.ShapefileWorkspaceFactory;
import com.esri.arcgis.geodatabase.DEFeatureClass;
import com.esri.arcgis.geodatabase.DEFeatureClassType;
import com.esri.arcgis.geodatabase.DETable;
import com.esri.arcgis.geodatabase.DETableType;
import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClassProxy;
import com.esri.arcgis.geodatabase.IFeatureWorkspace;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.IWorkspaceFactory;
import com.esri.arcgis.geodatabase.Table;
import com.esri.arcgis.geodatabase.Workspace;
import com.esri.arcgis.geodatabase.esriFeatureType;
import com.esri.arcgis.geometry.ISpatialReference;
import com.esri.arcgis.geometry.ISpatialReferenceAuthority;
import com.esri.arcgis.geometry.esriSRGeoCSType;
import com.esri.arcgis.geoprocessing.BaseGeoprocessingTool;
import com.esri.arcgis.geoprocessing.GPBoolean;
import com.esri.arcgis.geoprocessing.GPBooleanType;
import com.esri.arcgis.geoprocessing.GPCompositeDataType;
import com.esri.arcgis.geoprocessing.GPDouble;
import com.esri.arcgis.geoprocessing.GPDoubleType;
import com.esri.arcgis.geoprocessing.GPFeatureLayer;
import com.esri.arcgis.geoprocessing.GPFeatureSchema;
import com.esri.arcgis.geoprocessing.GPParameter;
import com.esri.arcgis.geoprocessing.GPString;
import com.esri.arcgis.geoprocessing.GPStringType;
import com.esri.arcgis.geoprocessing.GPTableSchema;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.esriGPParameterDirection;
import com.esri.arcgis.geoprocessing.esriGPParameterType;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.system.Cleaner;
import com.esri.arcgis.system.IArray;
import com.esri.arcgis.system.IName;
import com.esri.arcgis.system.ITrackCancel;

import java.io.File;
import java.io.IOException;

/**
 */
public abstract class AbstractTool extends BaseGeoprocessingTool
{
    private final static SparkToolbox FACTORY = new SparkToolbox();

    @Override
    public IName getFullName() throws IOException, AutomationException
    {
        return (IName) FACTORY.getFunctionName(getName());
    }

    @Override
    public boolean isLicensed() throws IOException, AutomationException
    {
        return true;
    }

    @Override
    public void updateMessages(
            final IArray parameters,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages)
    {
    }

    @Override
    public void updateParameters(
            final IArray parameters,
            final IGPEnvironmentManager environmentManager)
    {
    }

    @Override
    public String getMetadataFile() throws IOException, AutomationException
    {
        return null;
    }

    @Override
    public void execute(
            final IArray parameters,
            final ITrackCancel trackCancel,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages) throws IOException, AutomationException
    {
        try
        {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

            doExecute(parameters, trackCancel, environmentManager, messages);
        }
        catch (Throwable t)
        {
            messages.addAbort(t.toString());
            for (final StackTraceElement stackTraceElement : t.getStackTrace())
            {
                messages.addAbort(stackTraceElement.toString());
            }
        }
    }

    protected void addParamDouble(
            final IArray parameters,
            final String displayName,
            final String name,
            final double value) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new GPDoubleType());
        final GPDouble gpDouble = new GPDouble();
        gpDouble.setValue(value);
        parameter.setValueByRef(gpDouble);
        parameters.add(parameter);
    }

    protected void addParamBoolean(
            final IArray parameters,
            final String displayName,
            final String name,
            final Boolean value) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new GPBooleanType());
        final GPBoolean gpBoolean = new GPBoolean();
        gpBoolean.setValue(value);
        parameter.setValueByRef(gpBoolean);
        parameters.add(parameter);
    }

    protected GPParameter addParamString(
            final IArray parameters,
            final String displayName,
            final String name,
            final String value) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new GPStringType());
        final GPString gpString = new GPString();
        gpString.setValue(value);
        parameter.setValueByRef(gpString);
        parameters.add(parameter);
        return parameter;
    }

    protected void addParamFolder(
            final IArray parameters,
            final String displayName,
            final String name,
            final String catalogPath
    ) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new DEFolderType());
        final DEFolder folder = new DEFolder();
        folder.setCatalogPath(catalogPath);
        parameter.setValueByRef(folder);
        parameters.add(parameter);
    }

    protected void addParamFile(
            final IArray parameters,
            final String displayName,
            final String name,
            final String catalogPath
    ) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new DEFileType());
        final DEFile file = new DEFile();
        file.setCatalogPath(catalogPath);
        parameter.setValueByRef(file);
        parameters.add(parameter);
    }

    protected void addParamFeatureClassIn(
            final IArray parameters,
            final String displayName,
            final String name) throws IOException
    {
        final GPCompositeDataType compositeDataType = new GPCompositeDataType();
        // compositeDataType.addDataType(new GPFeatureLayerType());
        compositeDataType.addDataType(new DEFeatureClassType());
        // compositeDataType.addDataType(new GPLayerType());
        // compositeDataType.addDataType(new DELayerType());
        // compositeDataType.addDataType(new GPFeatureRecordSetLayerType());

        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(compositeDataType);
        parameter.setValueByRef(new GPFeatureLayer());
        parameters.add(parameter);
    }

    protected void addParamFeatureClassOut(
            final IArray parameters,
            final String displayName,
            final String name,
            final String defaultValue) throws IOException
    {
        final GPFeatureSchema featureSchema = new GPFeatureSchema();
        featureSchema.setCloneDependency(true);

        GPParameter parameterOut = new GPParameter();
        parameterOut.setDirection(esriGPParameterDirection.esriGPParameterDirectionOutput);
        parameterOut.setDisplayName(displayName);
        parameterOut.setName(name);
        parameterOut.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameterOut.setDataTypeByRef(new DEFeatureClassType());
        final DEFeatureClass featureClass = new DEFeatureClass();
        featureClass.setAsText(defaultValue);
        parameterOut.setValueByRef(featureClass);
        parameterOut.setSchemaByRef(featureSchema);

        parameters.add(parameterOut);
    }

    protected void addParamTable(
            final IArray parameters,
            final String displayName,
            final String name,
            final String value) throws IOException
    {
        final GPTableSchema tableSchema = new GPTableSchema();
        tableSchema.setCloneDependency(true);

        final GPParameter parameterOut = new GPParameter();
        parameterOut.setDirection(esriGPParameterDirection.esriGPParameterDirectionOutput);
        parameterOut.setDisplayName(displayName);
        parameterOut.setName(name);
        parameterOut.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameterOut.setDataTypeByRef(new DETableType());
        final DETable table = new DETable();
        table.setAsText(value);
        parameterOut.setValueByRef(table);
        parameterOut.setSchemaByRef(tableSchema);

        parameters.add(parameterOut);
    }

    protected int getWkid(final FeatureClass featureClass) throws IOException
    {
        final int wkid;
        final ISpatialReference spatialReference = featureClass.getSpatialReference();
        try
        {
            if (spatialReference instanceof ISpatialReferenceAuthority)
            {
                final ISpatialReferenceAuthority spatialReferenceAuthority = (ISpatialReferenceAuthority) spatialReference;
                final int code = spatialReferenceAuthority.getCode();
                wkid = code == 0 ? esriSRGeoCSType.esriSRGeoCS_WGS1984 : code;
            }
            else
            {
                wkid = esriSRGeoCSType.esriSRGeoCS_WGS1984;
            }
        }
        finally
        {
            Cleaner.release(spatialReference);
        }
        return wkid;
    }

    protected Table createTable(
            final String tablePath,
            final IFields fields,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws IOException
    {
        final File outputFCFile = new File(tablePath);
        File outputWSFile = outputFCFile.getParentFile();

        if (outputWSFile == null)
        {
            final String scratchWorkspace = environmentManager.findEnvironment("scratchWorkspace").getValue().getAsText();
            if (scratchWorkspace != null && !scratchWorkspace.isEmpty())
            {
                outputWSFile = new File(scratchWorkspace);
            }
            else
            {
                messages.addError(500, "No output folder or scratch workspace specified.");
                return null;
            }
        }

        String outputWSName = outputWSFile.getName();
        IWorkspaceFactory workspaceFactory;
        IFeatureWorkspace featureWorkspace;

        if (outputWSFile.exists() && outputWSFile.isDirectory())
        {
            // Now that output location is known, determine type of workspace, FileGDB or shapefile. SDE is not supported.
            if (outputWSName.endsWith(".gdb"))
            {
                workspaceFactory = new FileGDBWorkspaceFactory();
                outputWSName = outputWSName.substring(0, outputWSName.indexOf(".gdb"));
            }
            else
            {
                workspaceFactory = new ShapefileWorkspaceFactory();
            }
            // Create the output workspace if it's not created yet. If the output file gdb exists, then workspace exists too.
            if (!workspaceFactory.isWorkspace(outputWSFile.getAbsolutePath()))
            {
                workspaceFactory.create(outputWSFile.getParentFile().getAbsolutePath(), outputWSName, null, 0);
            }
            // Retrieve the output workspace
            featureWorkspace = new Workspace(workspaceFactory.openFromFile(outputWSFile.getAbsolutePath(), 0));
        }
        else
        {
            messages.addError(500, "Output or scratch workspace directory does not exist.");
            return null;
        }

        return new Table(featureWorkspace.createTable(outputFCFile.getName(), fields, null, null, "default"));
    }

    protected FeatureClass createFeatureClass(
            final String featureClassName,
            final IFields fields,
            final String shapeFieldName,
            final IGPEnvironmentManager manager,
            final IGPMessages messages) throws Exception
    {
        // Extract parent folder of output feature class or shapefile provided as output parameter
        final File outputFCFile = new File(featureClassName);

        File outputWSFile = outputFCFile.getParentFile();

        //if only a file name is provided without parent folder or absolute path, use the scratch workspace as output folder
        if (outputWSFile == null)
        {
            final String scratchWorkspace = manager.findEnvironment("scratchWorkspace").getValue().getAsText();
            if (scratchWorkspace != null && !scratchWorkspace.isEmpty())
            {
                outputWSFile = new File(scratchWorkspace);
            }
            else
            {
                messages.addError(500, "No output folder or scratch workspace specified.");
                return null;
            }
        }

        final IWorkspaceFactory workspaceFactory;
        final IFeatureWorkspace featureWorkspace;

        String outputWSName = outputWSFile.getName();
        if (outputWSFile.exists() && outputWSFile.isDirectory())
        {
            // Now that output location is known, determine type of workspace, FileGDB or shapefile. SDE is not supported.
            if (outputWSName.endsWith(".gdb"))
            {
                workspaceFactory = new FileGDBWorkspaceFactory();
                outputWSName = outputWSName.substring(0, outputWSName.indexOf(".gdb"));
            }
            else
            {
                workspaceFactory = new ShapefileWorkspaceFactory();
            }
            // Create the output workspace if it's not created yet. If the output file gdb exists, then workspace exists too.
            if (!workspaceFactory.isWorkspace(outputWSFile.getAbsolutePath()))
            {
                workspaceFactory.create(outputWSFile.getParentFile().getAbsolutePath(), outputWSName, null, 0);
            }
            // Retrieve the output workspace
            featureWorkspace = new Workspace(workspaceFactory.openFromFile(outputWSFile.getAbsolutePath(), 0));
        }
        else
        {
            messages.addError(500, "Output or scratch workspace directory does not exist.");
            return null;
        }

        return new FeatureClass(featureWorkspace.createFeatureClass(outputFCFile.getName(), fields, null, null,
                esriFeatureType.esriFTSimple, shapeFieldName, "default"));
    }

    protected FeatureClass toFeatureClass(final IGPValue value) throws IOException
    {
        final IFeatureClass[] featureClasses = new IFeatureClass[]{new IFeatureClassProxy()};
        gpUtilities.decodeFeatureLayer(value, featureClasses, null);
        return new FeatureClass(featureClasses[0]);
    }

    protected abstract void doExecute(
            final IArray parameters,
            final ITrackCancel trackCancel,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages) throws Throwable;

}
