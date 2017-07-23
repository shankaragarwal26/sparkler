package edu.usc.irds.sparkler;

/**
 * Created by shankaragarwal on 04/07/17.
 */
public class Constants {
    static final String CONSUMER_TOPIC_NAME = "inell_sparkler_test";
//    public static final String bootStrapServerName = "10.24.28.111:6667";
    public static final String bootStrapServerName = "127.0.0.1:9092";
    static final String CONSUMER_GROUP_ID = "inell_sparkler_group";

    static final int  corePoolSize  =    5;
    static final int  maxPoolSize   =   10;
    static final long keepAliveTime = 5000;


    static final int MAX_QUEUE_SIZE = 100;
    static final int MAX_BATCH_SIZE = 20;
    static final int MAX_THREAD_COUNT = 10;
    static final int SLEEP_TIME = 50000;
}