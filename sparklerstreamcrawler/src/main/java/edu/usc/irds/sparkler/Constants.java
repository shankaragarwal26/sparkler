package edu.usc.irds.sparkler;

/**
 * Created by shankaragarwal on 04/07/17.
 */
public class Constants {
    static final String CONSUMER_TOPIC_NAME_KEY = "topic-name";
    public static final String BOOTSTRAP_SERVER_KEY = "bootstrap-server";
    static final String CONSUMER_GROUP_KEY = "consumer_group_id";
    public static final String SPARKLER_APP_JAR_PATH_KEY = "sparkler-app-jar-path";

    static final int  corePoolSize  =    5;
    static final int  maxPoolSize   =   10;
    static final long keepAliveTime = 5000;


    static final int MAX_BATCH_SIZE = 20;
    static final int MAX_THREAD_COUNT = 1;
    static final int SLEEP_TIME = 50000;
    public static final long MINUTES = 10;

}