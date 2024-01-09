package com;


import com.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.TimeUnit;

public class Producer implements Runnable{
    public Producer(){
    }

    public void produce() throws InterruptedException, ClassNotFoundException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(Config.kafkaClientConf());

        ProducerRecord<String, String> record;
        while (true) {
            System.out.println("生产");
            record = new ProducerRecord<>(Config.KAFKA_TOPIC, "key", "Hello, Kafka!");
            producer.send(record);
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
