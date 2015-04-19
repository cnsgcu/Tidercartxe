package web.journal.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class ApplicationConfig
{
    final static private Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);

    @Bean
    public ScheduledExecutorService scheduledExecutorService()
    {
        LOGGER.info("Thread pool of 4 threads");

        return Executors.newScheduledThreadPool(4);
    }

//    TODO active NLP when everything else is done
//    @Bean
//    public StanfordCoreNLP coreNLP()
//    {
//        final Properties props = new Properties();
//
//        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment");
//
//        return new StanfordCoreNLP(props);
//    }
}