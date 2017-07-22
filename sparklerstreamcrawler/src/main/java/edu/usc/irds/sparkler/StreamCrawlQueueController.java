package edu.usc.irds.sparkler;

import edu.usc.irds.sparkler.utils.KafkaConsumerController;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static edu.usc.irds.sparkler.Constants.corePoolSize;
import static edu.usc.irds.sparkler.Constants.keepAliveTime;
import static edu.usc.irds.sparkler.Constants.maxPoolSize;

/**
 * Created by shankaragarwal on 21/07/17.
 */
public class StreamCrawlerURLController {

    static StreamCrawlerURLController streamCrawlerURLController = null;

    private KafkaConsumerController kafkaConsumerController = null;

    private ExecutorService threadPoolExecutor;

    private int MAX_QUEUE_SIZE = 100; //TODO Need Fine Tuning

    private Queue<CrawlURLData> crawlURLDataQueue;

    private StreamCrawlerURLController(){
        kafkaConsumerController = new KafkaConsumerController();

        crawlURLDataQueue = new LinkedList<CrawlURLData>();


        threadPoolExecutor =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maxPoolSize,
                        keepAliveTime,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>()
                );

        kafkaConsumerController.startListenting();
    }

    public static StreamCrawlerURLController getInstance(){

        synchronized (StreamCrawlerURLController.class) {
            if (streamCrawlerURLController == null) {
                streamCrawlerURLController = new StreamCrawlerURLController();
            }
        }
        return streamCrawlerURLController;
    }

    public void handleNewURLMessage(final String urlMessage){
        //Use Thread Pool Executors
        threadPoolExecutor.execute(new Runnable() {
            public void run() {
                insertURLForCrawl(urlMessage);
                checkForThreadCount();
            }
        });

        if(threadPoolExecutor.getActiveCount() >= MAX_THRESHOLD)
            kafkaConsumerController.stopListening();
    }

    private void checkForThreadCount() {
        synchronized (this){
            if(threadPoolExecutor.getActiveCount() < MAX_THRESHOLD && !kafkaConsumerController.isListening())
                kafkaConsumerController.startListenting();
        }
    }

    private void insertURLForCrawl(String urlMessage) {
        //Handle Messages and Insert Into Crawlerx
    }
}
