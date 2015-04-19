package web.journal.service.bolt;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import web.journal.service.tools.Rankable;
import web.journal.service.tools.RankableObjectWithFields;

public final class IntermediateRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOGGER = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt()
    {
        super();
    }

    public IntermediateRankingsBolt(int topN)
    {
        super(topN);
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds)
    {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple)
    {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        super.getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}