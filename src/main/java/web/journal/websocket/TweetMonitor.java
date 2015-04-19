package web.journal.websocket;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.server.standard.SpringConfigurator;

import javax.annotation.Resource;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ServerEndpoint(
    value = "/ws/statistic",
    configurator = SpringConfigurator.class
)
public class TweetMonitor
{
    @Resource
    private ScheduledExecutorService SCHEDULER;

    private Session session;

    final private Gson gson = new Gson();

    final private Logger LOGGER = LoggerFactory.getLogger(TweetMonitor.class);

    private List<String> topics = Arrays.asList(
        "comments4kids",
        "edchat",
        "edchatie",
        "edtech",
        "eltchat",
        "gamemooc",
        "mlearning",
        "Satchat",
        "SLpeeps",
        "Spedchat",
        "Titletalk",
        "Tlchat"
    );

    @OnOpen
    public void init(Session session)
    {
        this.session = session;

        SCHEDULER.scheduleWithFixedDelay(this::processTweet, 0, 1, TimeUnit.SECONDS);
    }

    @OnMessage
    public void listen(String msg)
    {
        LOGGER.info(msg);
    }

    @OnClose
    public void close() throws IOException {}

    @OnError
    public void handleError(Throwable throwable) throws IOException
    {
        this.session.close();

        throwable.printStackTrace();
    }

    private void processTweet()
    {
        try {
            final TweetStatistic tweetStatistic = new TweetStatistic();

            Collections.shuffle(topics);

            final List<TweetInfo> tweetInfos = topics.stream()
                .limit(7)
                .map(topic -> {
                    final TweetInfo tweetInfo = new TweetInfo();
                    tweetInfo.setCount((new Random()).nextInt(10));
                    tweetInfo.setTopic(topic);

                    return tweetInfo;
                })
                .collect(Collectors.toList());

            tweetStatistic.setTotal((new Random()).nextInt(100));
            tweetStatistic.setTweetInfos(tweetInfos);

            this.session.getBasicRemote().sendText(gson.toJson(tweetStatistic));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class TweetStatistic
    {
        private long total;

        private List<TweetInfo> tweetInfos;

        public long getTotal()
        {
            return total;
        }

        public void setTotal(long total)
        {
            this.total = total;
        }

        public List<TweetInfo> getTweetInfos()
        {
            return tweetInfos;
        }

        public void setTweetInfos(List<TweetInfo> tweetInfos)
        {
            this.tweetInfos = tweetInfos;
        }
    }

    private class TweetInfo
    {
        private int count;

        private String topic;

        public int getCount()
        {
            return count;
        }

        public void setCount(int count)
        {
            this.count = count;
        }

        public String getTopic()
        {
            return topic;
        }

        public void setTopic(String topic)
        {
            this.topic = topic;
        }
    }
}
