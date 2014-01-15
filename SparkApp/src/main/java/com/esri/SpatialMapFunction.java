package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 */
final class SpatialMapFunction extends PairFlatMapFunction<Tuple2<LongWritable, Text>, Integer, Integer>
{
    private final static double EPS = 0.0000001;
    private final SpatialIndex m_spatialIndex;
    private final Coordinate m_coordinate = new Coordinate();
    private final Envelope m_envelope = new Envelope();
    private final FastTok m_fastTok = new FastTok();
    private final List<Tuple2<Integer, Integer>> m_list = new ArrayList<Tuple2<Integer, Integer>>();

    public SpatialMapFunction(final Broadcast<SpatialIndex> spatialIndexBroadcast)
    {
        m_spatialIndex = spatialIndexBroadcast.value();
    }

    @Override
    public Iterable<Tuple2<Integer, Integer>> call(final Tuple2<LongWritable, Text> tuple2) throws Exception
    {
        m_list.clear();
        final int count = m_fastTok.tokenize(tuple2._2().toString(), ',');
        if (count == 3)
        {
            m_coordinate.x = Double.parseDouble(m_fastTok.tokens[1]);
            m_coordinate.y = Double.parseDouble(m_fastTok.tokens[2]);
            final Point point = GeomFact.createPoint(m_coordinate);
            m_envelope.init(point.getX() - EPS, point.getX() + EPS, point.getY() - EPS, point.getY() + EPS);
            final List list = m_spatialIndex.query(m_envelope);
            for (final Object obj : list)
            {
                final PreparedFeature preparedFeature = (PreparedFeature) obj;
                if (preparedFeature.preparedGeometry.covers(point))
                {
                    m_list.add(new Tuple2<Integer, Integer>(preparedFeature.id, 1));
                    break;
                }
            }
        }
        return m_list;
    }
}
