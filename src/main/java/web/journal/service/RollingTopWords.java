package web.journal.service;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import web.journal.service.bolt.*;
import web.journal.service.spout.TwitterSpout;
import web.journal.service.util.StormRunner;

public class RollingTopWords
{
    private static final Logger LOGGER = Logger.getLogger(RollingTopWords.class);

    private static final int TOP_N = 7;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;

    public RollingTopWords(String topologyName) throws InterruptedException
    {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();

        wireTweetTopology();
    }

    private static Config createTopologyConfiguration()
    {
        Config conf = new Config();
        conf.setDebug(true);
        //conf.put(Config.TOPOLOGY_DEBUG, false);

        return conf;
    }

    private void wireTweetTopology()
    {
        final String tweetStream = "tweetStream";
        final String topicId = "tweetTopic";
        final String counterId = "counter";
        final String intermediateRankerId = "intermediateRanker";
        final String totalRankerId = "finalRanker";

        builder.setSpout(
            tweetStream,
            new TwitterSpout(
                "",
                "",
                "",
                "",
                new String[] {}
            )
        );

        builder.setBolt(
            topicId,
            new TweetTopicBolt()
        ).fieldsGrouping(tweetStream, new Fields("tweet"));

        builder.setBolt(
            counterId,
            new RollingCountBolt(9, 3)
        ).fieldsGrouping(topicId, new Fields("tweet-topic"));

        builder.setBolt(
            intermediateRankerId,
            new IntermediateRankingsBolt(TOP_N)
        ).fieldsGrouping(counterId, new Fields("obj"));

        builder.setBolt(
            totalRankerId,
            new TotalRankingsBolt(TOP_N)
        ).fieldsGrouping(intermediateRankerId, new Fields("rankings"));

        builder.setBolt(
            "reporter",
            new ReportBolt()
        ).fieldsGrouping(totalRankerId, new Fields("rankings"));
    }

    public void runLocally() throws InterruptedException
    {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig);
    }
}