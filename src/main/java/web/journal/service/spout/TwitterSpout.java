package web.journal.service.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream twitterStream;

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keyWords;

    public TwitterSpout(
        String consumerKey, String consumerSecret,
        String accessToken, String accessTokenSecret, String[] keyWords)
    {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        queue = new LinkedBlockingQueue<>(1000);
        this.collector = collector;

        final StatusListener listener = new StatusListener()
        {
            @Override
            public void onStatus(Status status)
            {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {}

            @Override
            public void onTrackLimitationNotice(int i) {}

            @Override
            public void onScrubGeo(long l, long l1) {}

            @Override
            public void onException(Exception ex) {}

            @Override
            public void onStallWarning(StallWarning arg0) {}
        };

        final AccessToken token = new AccessToken(accessToken, accessTokenSecret);

        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build()
        ).getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        twitterStream.setOAuthAccessToken(token);

        if (keyWords.length == 0) {
            twitterStream.sample();
        } else {
            final FilterQuery query = new FilterQuery().track(keyWords);

            twitterStream.filter(query);
        }
    }

    @Override
    public void nextTuple()
    {
        final Status status = queue.poll();

        if (status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

    @Override
    public void close()
    {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        final Config conf = new Config();
        conf.setMaxTaskParallelism(1);

        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void ack(Object id) {}

    @Override
    public void fail(Object id) {}
}