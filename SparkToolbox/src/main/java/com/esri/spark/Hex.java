package com.esri.spark;

import com.esri.arcgis.geometry.GeometryEnvironment;
import com.esri.arcgis.geometry.Polygon;
import com.esri.arcgis.geometry.Polyline;
import com.esri.arcgis.system._WKSPoint;

import java.io.IOException;

/**
 */
public final class Hex
{
    private double[] ox = new double[6];
    private double[] oy = new double[6];

    public final double h;
    public final double v;
    public final double w;
    public final double w2;

    public Hex(final double size)
    {
        h = 2.0 * size;
        v = 3.0 * h / 4.0;
        final double nume = Math.sqrt(3) * h;
        w = nume / 2.0;
        w2 = nume / 4.0;

        for (int i = 0; i < 6; i++)
        {
            final double angle = 2.0 * Math.PI / 6.0 * (i + 0.5);
            ox[i] = size * Math.cos(angle);
            oy[i] = size * Math.sin(angle);
        }
    }

    public Polyline toPolyline(
            final GeometryEnvironment geometryEnv,
            final double x,
            final double y
    ) throws IOException
    {
        final Polyline polyline = new Polyline();
        geometryEnv.setWKSPoints(polyline, getWksPoints(x, y));
        return polyline;
    }

    public Polygon toPolygon(
            final GeometryEnvironment geometryEnv,
            final double x,
            final double y
    ) throws IOException
    {
        final Polygon polygon = new Polygon();
        geometryEnv.setWKSPoints(polygon, getWksPoints(x, y));
        return polygon;

    }

    private _WKSPoint[] getWksPoints(
            final double x,
            final double y)
    {
        final _WKSPoint[] points = new _WKSPoint[7];
        for (int i = 0; i < 6; i++)
        {
            final _WKSPoint point = new _WKSPoint();
            point.x = x + ox[i];
            point.y = y + oy[i];
            points[i] = point;
        }
        final _WKSPoint point = new _WKSPoint();
        point.x = x + ox[0];
        point.y = y + oy[0];
        points[6] = point;

        return points;
    }
}
