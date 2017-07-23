package edu.usc.irds.sparkler;

/**
 * Created by shankaragarwal on 04/07/17.
 */
public class Constants {
    static final String CONSUMER_TOPIC_NAME_KEY = "topic-name";
    public static final String BOOTSTRAP_SERVER_KEY = "bootstrap-server";
    static final String CONSUMER_GROUP_KEY = "consumer_group_id";

    static final int  corePoolSize  =    5;
    static final int  maxPoolSize   =   10;
    static final long keepAliveTime = 5000;


    static final int MAX_QUEUE_SIZE = 100;
    static final int MAX_BATCH_SIZE = 20;
    static final int MAX_THREAD_COUNT = 10;
    static final int SLEEP_TIME = 50000;
}