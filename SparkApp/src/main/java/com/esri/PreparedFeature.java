package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 */
public final class PreparedFeature implements Externalizable, Serializable
{
    public PreparedGeometry preparedGeometry;
    public int id;

    public PreparedFeature()
    {
    }

    public PreparedFeature(
            final PreparedGeometry preparedGeometry,
            final Integer id)
    {
        this.preparedGeometry = preparedGeometry;
        this.id = id;
    }

    @Override
    public void writeExternal(final ObjectOutput output) throws IOException
    {
        output.writeInt(this.id);
        final Geometry geometry = this.preparedGeometry.getGeometry();
        final Coordinate[] coordinates = geometry.getCoordinates();
        output.writeInt(coordinates.length);
        for (final Coordinate coordinate : coordinates)
        {
            output.writeDouble(coordinate.x);
            output.writeDouble(coordinate.y);
        }
    }

    @Override
    public void readExternal(final ObjectInput input) throws IOException, ClassNotFoundException
    {
        this.id = input.readInt();
        final int length = input.readInt();
        final Coordinate[] coordinates = new Coordinate[length];
        for (int i = 0; i < length; i++)
        {
            final double x = input.readDouble();
            final double y = input.readDouble();
            coordinates[i] = new Coordinate(x, y);
        }
        this.preparedGeometry = PreparedGeometryFactory.prepare(GeomFact.createPolygon(coordinates));
    }
}
