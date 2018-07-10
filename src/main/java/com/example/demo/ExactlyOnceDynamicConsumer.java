package com.example.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExactlyOnceDynamicConsumer {
    private static OffsetManager offsetManager = new OffsetManager("storage2");

    public static void main(String[] str) throws InterruptedException {
        System.out.println("Starting ExactlyOnceDynamicConsumer ...");
        readMessages();
    }

    private static void readMessages() throws InterruptedException {
        KafkaConsumer<String, String> consumer = createConsumer();
        // Manually controlling offset but register consumer to topics to get dynamically
        //  assigned partitions. Inside MyConsumerRebalancerListener use
        // consumer.seek(topicPartition,offset) to control offset which messages to be read.
        consumer.subscribe(Arrays.asList("normal-topic"),
                new MyConsumerRebalancerListener(consumer));
        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        String consumeGroup = "cg3";
        props.put("group.id", consumeGroup);
        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");
        // Control maximum data on each poll, make sure this value is bigger than the maximum                   // single message size
        props.put("max.partition.fetch.bytes", "140");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                // Save processed offset in external storage.
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());
            }
        }
    }
}

class MyConsumerRebalancerListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
    private OffsetManager offsetManager = new OffsetManager("storage2");
    private Consumer<String, String> consumer;

    public MyConsumerRebalancerListener(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
    }
}

/**
 * The partition offset are stored in an external storage. In this case in a local file system where
 * program runs.
 */
class OffsetManager {
    private String storagePrefix;

    public OffsetManager(String storagePrefix) {
        this.storagePrefix = storagePrefix;
    }

    /**
     * Overwrite the offset for the topic in an external storage.
     *
     * @param topic     - Topic name.
     * @param partition - Partition of the topic.
     * @param offset    - offset to be stored.
     */
    void saveOffsetInExternalStore(String topic, int partition, long offset) {
        try {
            FileWriter writer = new FileWriter(storageName(topic, partition), false);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(offset + "");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * @return he last offset + 1 for the provided topic and partition.
     */
    long readOffsetFromExternalStore(String topic, int partition) {
        try {
            Stream<String> stream = Files.lines(Paths.get(storageName(topic, partition)));
            return Long.parseLong(stream.collect(Collectors.toList()).get(0)) + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private String storageName(String topic, int partition) {
        return storagePrefix + "-" + topic + "-" + partition;
    }
}