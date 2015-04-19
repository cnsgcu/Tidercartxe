package web.journal.service.bolt;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import web.journal.service.tools.Rankings;

public final class TotalRankingsBolt extends AbstractRankerBolt
{
    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOGGER = Logger.getLogger(TotalRankingsBolt.class);

    public TotalRankingsBolt()
    {
        super();
    }

    public TotalRankingsBolt(int topN)
    {
        super(topN);
    }

    public TotalRankingsBolt(int topN, int emitFrequencyInSeconds)
    {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple)
    {
        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        super.getRankings().updateWith(rankingsToBeMerged);
        super.getRankings().pruneZeroCounts();

        // Push to Kafka
        getRankings().getRankings().stream().forEach(
                r -> System.out.println("data: " + r.getObject().toString() + " - " + r.getCount() + " \n\n")
        );
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }

}
