package web.journal.service;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import web.journal.service.bolt.IntermediateRankingsBolt;
import web.journal.service.bolt.RollingCountBolt;
import web.journal.service.bolt.TotalRankingsBolt;
import web.journal.service.bolt.TweetTopicBolt;
import web.journal.service.spout.TwitterSpout;
import web.journal.service.util.StormRunner;

public class RollingTopWords
{
    private static final Logger LOGGER = Logger.getLogger(RollingTopWords.class);

    private static final int TOP_N = 10;
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 30;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public RollingTopWords(String topologyName) throws InterruptedException
    {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTweetTopology();
    }

    private static Config createTopologyConfiguration()
    {
        Config conf = new Config();
        conf.setDebug(true);

        return conf;
    }

    private void wireTweetTopology()
    {
        final String tweetStream = "tweetStream";
        final String topicId = "tweetTopic";
        final String counterId = "counter";
        final String intermediateRankerId = "intermediateRanker";
        final String totalRankerId = "finalRanker";

        // TODO provide real credential
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
        ).globalGrouping(intermediateRankerId);
    }

    public void runLocally() throws InterruptedException
    {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

//    public static void main(String[] args) throws Exception
//    {
//        final String topologyName = "slidingWindowCounts";
//
//        LOG.info("Start topology: " + topologyName);
//        RollingTopWords rtw = new RollingTopWords(topologyName);
//
//        rtw.runLocally();
//    }
}