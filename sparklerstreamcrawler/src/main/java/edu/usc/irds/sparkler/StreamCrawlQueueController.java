package edu.usc.irds.sparkler;

import edu.usc.irds.sparkler.pipeline.Crawler;
import edu.usc.irds.sparkler.service.Injector;
import edu.usc.irds.sparkler.utils.KafkaConsumerController;
import edu.usc.irds.sparkler.utils.KafkaConsumerHandler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static edu.usc.irds.sparkler.Constants.*;

/**
 * Created by shankaragarwal on 21/07/17.
 */
public class StreamCrawlQueueController implements KafkaConsumerHandler {

    private static StreamCrawlQueueController streamCrawlerURLController = null;

    private KafkaConsumerController kafkaConsumerController = null;

    private String consumerGroup;
    private String topic;


    private StreamCrawlQueue crawlURLDataQueue;

    private StreamCrawlQueueController(){
        kafkaConsumerController = new KafkaConsumerController(this);
        HashMap<String,String> kafkaConfiguration = (HashMap<String, String>) SparklerStreamConfiguration.getInstance().getValue("kafka");
        topic = kafkaConfiguration.get(Constants.CONSUMER_TOPIC_NAME_KEY);
        consumerGroup = kafkaConfiguration.get(Constants.CONSUMER_GROUP_KEY);

        crawlURLDataQueue = new StreamCrawlQueue();

        new Thread(new Runnable() {
            public void run() {
              getItemsFromQueue();
            }
        }).start();

        kafkaConsumerController.startListenting(consumerGroup, topic);
    }

    private void getItemsFromQueue() {

        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maxPoolSize,
                        keepAliveTime,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>()
                );

        while(true){
            if(threadPoolExecutor.getActiveCount()>=MAX_THREAD_COUNT) {
                try {
                    Thread.sleep(SLEEP_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                final Set<CrawlURLData> seedURLS = new HashSet<CrawlURLData>();
                while(seedURLS.size()<MAX_BATCH_SIZE && crawlURLDataQueue.getSize()>0) {
                    CrawlURLData crawlURLData = crawlURLDataQueue.getElement();
                    seedURLS.add(crawlURLData);
                }
                if(seedURLS.size()>0) {
                    threadPoolExecutor.submit(() -> createASparklerBatch(seedURLS));
                }

            }

        }
    }

    private void createASparklerBatch(Set<CrawlURLData> seedURLS) {
        String[] args = new String[seedURLS.size()+1];
        args[0] = "-su";
        int i=1;
        for (CrawlURLData url:seedURLS){
            System.out.println("Injecting "+ url.getURL());
            args[i++] = url.getURL();
        }
        String jobId = Injector.start(args);
        System.out.print(jobId);


        if(jobId !=null && jobId.length()>0) {
            args = new String[4];
            args[0] = "-id";
            args[1] = jobId;
            args[2] = "-i";
            args[3] = "1";
            Crawler.main(args);
            System.out.print("Started job "+ jobId);
        }
    }

    public static StreamCrawlQueueController getInstance(){

        synchronized (StreamCrawlQueueController.class) {
            if (streamCrawlerURLController == null) {
                streamCrawlerURLController = new StreamCrawlQueueController();
            }
        }
        return streamCrawlerURLController;
    }

    private void checkForQueueSize() {
        synchronized (this){
            if(crawlURLDataQueue.getSize() >=MAX_QUEUE_SIZE && kafkaConsumerController.isListening())
                kafkaConsumerController.stopListening();
            else if(crawlURLDataQueue.getSize()< MAX_QUEUE_SIZE && !kafkaConsumerController.isListening())
                kafkaConsumerController.startListenting(consumerGroup, topic);
        }
    }

    public void handleMessage(String urlMessage) {
        crawlURLDataQueue.insertURL(urlMessage);
        checkForQueueSize();
    }
}
