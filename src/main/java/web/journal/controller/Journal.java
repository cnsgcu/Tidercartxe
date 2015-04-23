package web.journal.controller;

import com.google.gson.Gson;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import web.journal.domain.TweetStatistic;
import web.journal.service.RollingTopWords;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Controller
public class Journal
{
    final static private Logger LOGGER = LoggerFactory.getLogger(Journal.class);

    @Resource
    private ScheduledExecutorService SCHEDULER;

    final private Gson gson = new Gson();

    @RequestMapping("/")
    public String get()
    {
        SCHEDULER.schedule(
            () -> {
                try {
                    final String topologyName = "slidingWindowCounts";
                    LOGGER.info("Start topology: " + topologyName);

                    RollingTopWords rtw = new RollingTopWords(topologyName);

                    rtw.runLocally();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            },
            0, TimeUnit.SECONDS
        );

        return "analysis/page";
    }

    @RequestMapping("/tweet")
    public void analyze(HttpServletResponse response)
    {
        LOGGER.info("Get tweet info");

        response.setContentType("text/event-stream");

        try {
            final PrintWriter writer = response.getWriter();
            final KafkaTweetConsumer consumer = new KafkaTweetConsumer(writer);

            consumer.consume();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @RequestMapping("/statistic")
    public void report(HttpServletResponse response)
    {
        LOGGER.info("Get tweet statistic");

        response.setContentType("text/event-stream");

        try {
            final PrintWriter writer = response.getWriter();

            while (true) {
                final TweetStatistic tweetStatistic = new TweetStatistic();
                tweetStatistic.setTotal((new Random()).nextInt(100));

                writer.write("data: " + gson.toJson(tweetStatistic) + "\n\n");
                writer.flush();

                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    class KafkaTweetConsumer
    {
        final static String TOPIC = "tweet";

        final PrintWriter writer;
        final ConsumerConnector consumerConnector;

        public KafkaTweetConsumer(PrintWriter writer)
        {
            this.writer = writer;
            final Properties properties = new Properties();
            properties.put("zookeeper.connect","localhost:2181");
            properties.put("group.id","test-group");

            final ConsumerConfig consumerConfig = new ConsumerConfig(properties);
            consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        }

        public void consume()
        {
            final Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(TOPIC, 1);

            final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
            final KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);

            for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
                writer.write("data: " + new String(aStream.message()) + "\n\n");
                writer.flush();
            }
        }
    }
}
