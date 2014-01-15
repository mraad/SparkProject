package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import java.io.Serializable;

/**
 */
public final class GeomFact implements Serializable
{
    private static final long serialVersionUID = -992916060626003135L;

    public final static GeometryFactory instance = new GeometryFactory();

    public final static Polygon createPolygon(final Coordinate[] coordinates)
    {
        return instance.createPolygon(coordinates);
    }

    public final static Point createPoint(final Coordinate coordinate)
    {
        return instance.createPoint(coordinate);
    }
}
