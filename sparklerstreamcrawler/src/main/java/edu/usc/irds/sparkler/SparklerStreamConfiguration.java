package edu.usc.irds.sparkler;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by shankaragarwal on 23/07/17.
 */
public class SparklerStreamConfiguration {

    private static SparklerStreamConfiguration streamConfiguration = null;

    private Map<String,String> keyMap;

    private SparklerStreamConfiguration(){
        Yaml yaml = new Yaml();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream in = loader.getResourceAsStream("stream-crawler.yaml");
        keyMap = (Map<String, String>) yaml.load(in);
    }

    public Object getValue(String key){
        return keyMap.get(key);
    }

    public static SparklerStreamConfiguration getInstance(){
        synchronized (SparklerStreamConfiguration.class){
            if(streamConfiguration == null)
                streamConfiguration = new SparklerStreamConfiguration();
        }
        return streamConfiguration;
    }
}

