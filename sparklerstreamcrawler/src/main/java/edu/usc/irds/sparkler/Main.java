/**
 * Created by shankaragarwal on 21/07/17.
 */
package edu.usc.irds.sparkler;

public class Main {
    public static void main(String [] args){
        new Thread(new Runnable() {
            public void run() {
                StreamCrawlQueueController.getInstance();
            }
        }).start();
    }

}
