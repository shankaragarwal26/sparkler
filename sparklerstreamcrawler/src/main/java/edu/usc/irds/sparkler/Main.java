/**
 * Created by shankaragarwal on 21/07/17.
 */
package edu.usc.irds.sparkler;

import java.io.File;

public class Main {
    public static void main(String [] args){
        new Thread(new Runnable() {
            public void run() {
                SparklerStreamConfiguration  configuration = SparklerStreamConfiguration.getInstance();
                StreamCrawlQueueController.getInstance();
            }
        }).start();
    }

}
