package web.journal.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import web.journal.service.RollingTopWords;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Controller
public class Journal
{
    final static private Logger LOGGER = LoggerFactory.getLogger(Journal.class);

    @Resource
    private ScheduledExecutorService SCHEDULER;

    @RequestMapping("/")
    public String get()
    {
        return "analysis/page";
    }

    @RequestMapping("/tweet")
    public void analyze(HttpServletResponse response)
    {
        LOGGER.info("Get tweet info");

        response.setContentType("text/event-stream");

        try {
            PrintWriter writer = response.getWriter();

            // TODO Start this when web app starts
            SCHEDULER.schedule(
                () -> {
                    try {
                        final RollingTopWords topWords = new RollingTopWords("slidingWindowCounts");
                        topWords.runLocally();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                },
                0, TimeUnit.SECONDS
            );

            // TODO fetch data from Kafka then push back to clients
            while (true) {
                Thread.sleep(1000);
                writer.write("data: " + (LocalDateTime.now()) + "\n\n");
                writer.flush();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
