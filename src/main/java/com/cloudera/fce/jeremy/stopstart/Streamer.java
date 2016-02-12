package com.cloudera.fce.jeremy.stopstart;

import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class Streamer {

    public static void main(String[] args) throws Exception
    {
        
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        
        JavaStreamingContext ssc = createNewStreamingContext(sc);  
        ssc.start();
        System.out.println("Stream started!");
        
        Thread.sleep(30000);
        
        ssc.stop(false, true);
        System.out.println("Stream stopped!");
        
        Thread.sleep(10000);
        
        ssc = createNewStreamingContext(sc);
        ssc.start();
        System.out.println("Stream restarted!");
        
        ssc.awaitTermination();
        ssc.stop();
        
    }
    
    @SuppressWarnings("serial")
    private static JavaStreamingContext createNewStreamingContext(JavaSparkContext sc) {
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        
        Map<String, String> params = Maps.newHashMap();
        params.put("metadata.broker.list", "vm1:9092");
        Set<String> topics = Sets.newHashSet("topic1");
        
        JavaPairDStream<String, String> dstream = KafkaUtils.createDirectStream(
                ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, params, topics);
        
        dstream.map(new Function<Tuple2<String, String>, Void>() {
            @Override
            public Void call(Tuple2<String, String> message) throws Exception {
                System.out.println("Message: " + message._2);
                return null;
            }
        });
        
        dstream.count().foreachRDD(new Function<JavaRDD<Long>, Void>() {
            @Override
            public Void call(JavaRDD<Long> batchSize) throws Exception {
                System.out.println("Batch size: " + batchSize.collect().get(0));
                return null;
            }
        });
        
        return ssc;
    }
    
}
