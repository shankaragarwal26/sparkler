package edu.usc.irds.sparkler;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by shankaragarwal on 22/07/17.
 */
public class StreamCrawlQueue {

    private Queue<CrawlURLData> urlDataQueue;

    public  StreamCrawlQueue(){
        urlDataQueue = new LinkedList<CrawlURLData>();
    }

    public void insertURL(String url){
        CrawlURLData crawlURLData = new CrawlURLData(url);
        urlDataQueue.offer(crawlURLData);
    }

    public synchronized int getSize(){
        return urlDataQueue.size();
    }

    public synchronized  CrawlURLData getElement(){
        return urlDataQueue.poll();
    }


}
