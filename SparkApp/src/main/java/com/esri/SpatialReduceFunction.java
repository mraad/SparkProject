package com.esri;

import org.apache.spark.api.java.function.Function2;

/**
 */
final class SpatialReduceFunction extends Function2<Integer, Integer, Integer>
{
    private static final long serialVersionUID = -3706173733404928924L;

    @Override
    public Integer call(
            final Integer a,
            final Integer b) throws Exception
    {
        return a + b;
    }
}
