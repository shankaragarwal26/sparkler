package edu.usc.irds.sparkler;

import edu.usc.irds.sparkler.service.Injector;
import edu.usc.irds.sparkler.utils.KafkaConsumerController;
import edu.usc.irds.sparkler.utils.KafkaConsumerHandler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
    private String command;

    private String consumerGroup;
    private String topic;


    private StreamCrawlQueue crawlURLDataQueue;
    private String jarPath = null;

    private StreamCrawlQueueController(){
        kafkaConsumerController = new KafkaConsumerController(this);
        HashMap<String,String> kafkaConfiguration = (HashMap<String, String>) SparklerStreamConfiguration.getInstance().getValue("kafka");
        HashMap<String,String> sparklerConfiguration = (HashMap<String, String>) SparklerStreamConfiguration.getInstance().getValue("sparkler");
        topic = kafkaConfiguration.get(Constants.CONSUMER_TOPIC_NAME_KEY);
        consumerGroup = kafkaConfiguration.get(Constants.CONSUMER_GROUP_KEY);

        crawlURLDataQueue = new StreamCrawlQueue();

        new Thread(new Runnable() {
            public void run() {
              getItemsFromQueue();
            }
        }).start();

        this.jarPath = sparklerConfiguration.get(SPARKLER_APP_JAR_PATH_KEY);
        command ="java -jar "+jarPath + " crawl -id %s -i 1";


        kafkaConsumerController.startListenting(consumerGroup, topic);
        System.out.println("Started Listenining to topic" + topic);
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
            synchronized (this) {
                if (threadPoolExecutor.getActiveCount() < MAX_THREAD_COUNT) {
                    final Set<CrawlURLData> seedURLS = new HashSet<CrawlURLData>();
                    while (seedURLS.size() < MAX_BATCH_SIZE && crawlURLDataQueue.getSize() > 0) {
                        CrawlURLData crawlURLData = crawlURLDataQueue.getElement();
                        seedURLS.add(crawlURLData);
                    }
                    if (seedURLS.size() > 0) {
                        threadPoolExecutor.submit(() -> createASparklerBatch(seedURLS));
                    }

                }
            }

            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            String new_command = String.format(command, jobId);
            String line;
            try {
                System.out.println(new_command);
                Process p =Runtime.getRuntime().exec(new_command);
                InputStream iStream = p.getInputStream();

                long startTime = System.currentTimeMillis();

                BufferedReader reader = new BufferedReader(new InputStreamReader(iStream));
                while((line = reader.readLine())!=null){
                    System.out.println(line);
                }

                p.waitFor();

                long endTime = System.currentTimeMillis();
                double diff = (endTime-startTime)/(1000.0*60.0);
                System.out.print("Took "+ diff + " minutes");

                System.out.println("Output");

//                OutputStream outputStream = p.getOutputStream();
//                System.out.println(outputStream.toString());

//                String [] arg = new String [4];
//                arg[0] = "-id";
//                arg[1] = jobId;
//                arg[2] = "-i";
//                arg[3] = "2";
//                Crawler.main(arg);

            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
            }
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
//        synchronized (this){
//            if(crawlURLDataQueue.getSize() >=MAX_QUEUE_SIZE && kafkaConsumerController.isListening())
//                kafkaConsumerController.stopListening();
//            else if(crawlURLDataQueue.getSize()< MAX_QUEUE_SIZE && !kafkaConsumerController.isListening())
//                kafkaConsumerController.startListenting(consumerGroup, topic);
//        }
    }

    public void handleMessage(String urlMessage) {
        System.out.println("Adding url " + urlMessage);
        crawlURLDataQueue.insertURL(urlMessage);
//        checkForQueueSize();
    }
}
