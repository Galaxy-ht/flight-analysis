package com;

import com.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

public class Consumer implements Runnable{

    private JavaStreamingContext javaStreamingContext;

    public Consumer() throws UnknownHostException {
        javaStreamingContext = new JavaStreamingContext(Config.sparkConf(), Durations.seconds(Config.LISTEN_TIME));
    }

    public void start() throws InterruptedException {
        System.out.println("开始监听----");
        processData();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    private void processData(){
        System.out.println("开始接收并处理数据-----");

        Collection<String> topics = Collections.singletonList(Config.KAFKA_TOPIC);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, Config.streamingConnectKafkaConf())
                );

        JavaPairDStream<String, String> res = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        res.print();
    }

    @Override
    public void run() {
        try {
            start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
