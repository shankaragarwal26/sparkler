package edu.usc.irds.sparkler.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static edu.usc.irds.sparkler.Constants.bootStrapServerName;

/**
 * Created by shankaragarwal on 21/07/17.
 */
public class KafkaConsumerController implements KafkaConsumerNotifier {
    private boolean listenToKafka = false;
    private KafkaConsumerHandler consumerHandler;
    private Queue<KConsumer> consumerList;

    public KafkaConsumerController(KafkaConsumerHandler consumerHandler){
        this.consumerHandler = consumerHandler;
        consumerList = new LinkedList<KConsumer>();
    }
    public void startListenting(String consumerGroup, String topic){
        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        KConsumer consumer = new KConsumer(consumerGroup,
                topics,
                bootStrapServerName,
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
