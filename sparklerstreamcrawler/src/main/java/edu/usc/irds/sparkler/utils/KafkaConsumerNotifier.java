package edu.usc.irds.sparkler.utils;


/**
 * Created by shankaragarwal on 22/07/17.
 */
public interface KafkaConsumerNotifier {

    void readMessage(String message);
    void startedReadingMessage(KConsumer consumer);
    void stoppedReadingMessage(KConsumer consumer);

}
