package edu.usc.irds.sparkler.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private KafkaConsumerNotifier notifier;
    private boolean shouldListen;

    private static final int  corePoolSize  =    5;
    private static final int  maxPoolSize   =   10;
    private static final long keepAliveTime = 5000;

    private ExecutorService threadPoolExecutor =
            new ThreadPoolExecutor(
                    corePoolSize,
                    maxPoolSize,
                    keepAliveTime,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>()
            );
    public KConsumer(String groupId,
                     List<String> topics, String bootStrapServers,
                     KafkaConsumerNotifier notifier) {
        this.notifier = notifier;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers",bootStrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer(props);
    }

    public void startListening() {
        shouldListen = true;
        consumer.subscribe(topics);
        notifier.startedReadingMessage(this);

        final KConsumer currentObject = this;

        new Thread(new Runnable() {
            public void run() {
                while (shouldListen) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                        for (ConsumerRecord<String, String> record : records) {
                            handleMessage(record);
                        }
                    }
                    catch (WakeupException exp){

                    }
                }
                notifier.stoppedReadingMessage(currentObject);
            }
        }).start();
    }

    private void handleMessage(final ConsumerRecord<String, String> record) {

        threadPoolExecutor.execute(new Runnable() {
            public void run() {
                notifier.readMessage(record.value());
            }
        });
    }

    public void stopListening(){
        shouldListen = false;
    }

    public boolean isListening() {
        return shouldListen;
    }
}