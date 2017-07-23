package edu.usc.irds.sparkler.utils;

/**
 * Created by shankaragarwal on 17/07/17.
 */
public interface KafkaConsumerHandler {
    void handleMessage(String value);
}