package web.journal.service.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetTopicBolt extends BaseRichBolt
{
    private OutputCollector collector;

    private Pattern hashTagMatcher = Pattern.compile("#(.+?)\\b");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        final Status tweet = (Status) tuple.getValue(0);

        if (!tweet.isRetweet()) {
            Matcher matcher = hashTagMatcher.matcher(tweet.getText());

            while(matcher.find()) {
                collector.emit(new Values(matcher.group(1)));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet-topic"));
    }
}
