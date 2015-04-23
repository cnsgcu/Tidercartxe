package web.journal.service.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import web.journal.domain.TweetInfo;
import web.journal.service.tools.Rankings;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ReportBolt extends BaseRichBolt
{
    private Gson gson;
    private Producer<Integer, String> producer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        getGson();
        getKafkaProducer();
    }

    @Override
    public void execute(Tuple tuple)
    {
        final Rankings rankingsToReport = (Rankings) tuple.getValue(0);

        final List<TweetInfo> tweetInfos = rankingsToReport.getRankings().stream().map(
            r -> {
                TweetInfo ts = new TweetInfo();
                ts.setTopic(r.getObject().toString());
                ts.setCount(r.getCount());
                return ts;
            }
        ).collect(Collectors.toList());

        if (!tweetInfos.isEmpty()) {
            final String msg = getGson().toJson(tweetInfos);
            final Producer<Integer, String> producer = getKafkaProducer();

            producer.send(new KeyedMessage<>("tweet", msg));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("report-rankings"));
    }

    public Gson getGson()
    {
        if (gson == null) {
            gson = new Gson();
        }

        return gson;
    }

    public Producer<Integer, String> getKafkaProducer()
    {
        if (producer == null) {
            Properties props = new Properties();
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("metadata.broker.list", "localhost:9092");

            producer = new Producer<>(new ProducerConfig(props));
        }

        return producer;
    }
}
