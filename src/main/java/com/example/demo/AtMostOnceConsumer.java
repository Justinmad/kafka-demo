package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by Young.Z on 2018/7/10.
 */
public class AtMostOnceConsumer {

    public static void main(String[] str) throws InterruptedException {

        System.out.println("Starting AutoOffsetMostlyAtleastOnceButSometimeAtMostOnceConsumer ...");

        execute();

    }

    private static void execute() throws InterruptedException {

        KafkaConsumer<String, String> consumer = createConsumer();

        // Subscribe to all partition in that topic. 'assign' could be used here
        // instead of 'subscribe' to subscribe to specific partition.
        consumer.subscribe(Collections.singletonList("normal-topic"));

        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.0.102:9092,10.0.0.102:9091");
        String consumeGroup = "cg1";
        props.put("group.id", consumeGroup);

        // Set this property, if auto commit should happen.
        props.put("enable.auto.commit", "true");

        // Auto commit interval is an important property, kafka would commit offset at this interval.
        props.put("auto.commit.interval.ms", "101");

        // This is how to control number of records being read in each poll
        props.put("max.partition.fetch.bytes", "135");

        // Set this if you want to always read from beginning.
        //        props.put("auto.offset.reset", "earliest");

        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                lastOffset = record.offset();
            }

            System.out.println("lastOffset read: " + lastOffset);

            process();

        }
    }

    private static void process() throws InterruptedException {

        // create some delay to simulate processing of the record.
        Thread.sleep(500);
    }

}
