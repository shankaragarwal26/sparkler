package edu.usc.irds.sparkler.utils;

import edu.usc.irds.sparkler.SparklerStreamConfiguration;

import java.util.*;

import static edu.usc.irds.sparkler.Constants.BOOTSTRAP_SERVER_KEY;

/**
 * Created by shankaragarwal on 21/07/17.
 */
public class KafkaConsumerController implements KafkaConsumerNotifier {
    private boolean listenToKafka = false;
    private KafkaConsumerHandler consumerHandler;
    private Queue<KConsumer> consumerList;
    String bootStrapServer;

    public KafkaConsumerController(KafkaConsumerHandler consumerHandler){
        this.consumerHandler = consumerHandler;
        consumerList = new LinkedList<KConsumer>();
        @SuppressWarnings("unchecked") HashMap<String,String>  kafkaConfiguration = (HashMap<String, String>)
                SparklerStreamConfiguration.getInstance().getValue("kafka");
        bootStrapServer = kafkaConfiguration.get(BOOTSTRAP_SERVER_KEY);

    }
    public void startListenting(String consumerGroup, String topic){
        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        KConsumer consumer = new KConsumer(consumerGroup,
                topics,
                bootStrapServer,
                this
        );

        consumer.startListening();

    }

    public void stopListening(){
        synchronized (consumerList){
            KConsumer consumer = consumerList.peek();
            if(consumer !=null)
                consumer.stopListening();
        }
    }

    public boolean isListening() {
        synchronized (consumerList) {
            KConsumer consumer = consumerList.peek();
            if(consumer !=null)
                return consumer.isListening();
            return false;
        }
    }

    public void readMessage(String message) {
        if(consumerHandler!=null)
            consumerHandler.handleMessage(message);
    }

    public void startedReadingMessage(KConsumer consumer) {
        synchronized (consumerList) {
            try {
                consumerList.offer(consumer);
            }
            catch (Exception exp){

            }
        }
    }

    public void stoppedReadingMessage(KConsumer consumer) {
        synchronized (consumerList) {
            try {
                consumerList.remove(consumer);
            }
            catch (Exception exp){

            }
        }
    }
}
