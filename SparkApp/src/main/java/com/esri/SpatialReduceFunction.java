package com.esri;

import org.apache.spark.api.java.function.Function2;

/**
 */
final class SpatialReduceFunction extends Function2<Integer, Integer, Integer>
{
    @Override
    public Integer call(
            final Integer a,
            final Integer b) throws Exception
    {
        return a + b;
    }
}
