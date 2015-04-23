package web.journal.service.util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public final class TupleUtils
{
    private TupleUtils() {}

    public static boolean isTick(Tuple tuple)
    {
        return tuple != null
            && Constants.SYSTEM_COMPONENT_ID  .equals(tuple.getSourceComponent())
            && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }

}