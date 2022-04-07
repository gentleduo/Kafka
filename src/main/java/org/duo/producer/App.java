package org.duo.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class App {

    @Test
    public void producer() throws ExecutionException, InterruptedException {

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server01:9092,server02:9092,server03:9092");

        // kafka 持久化数据的MQ 数据 -> byte[]， 不会对数据进行干预，双方要约定编解码
        // kafka是一个app：使用零拷贝，sendfile系统调用实现快速数据消费
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 现在的producer就是一个提供者，面向的其实是broker，虽然在使用的时候我们期望把数据打入topic
        String topic = "gentleduo-topic-1";

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "item" + j, "val" + i);
                Future<RecordMetadata> send = producer.send(record);
                RecordMetadata rm = send.get();
                int partition = rm.partition();
                long offset = rm.offset();
                System.out.println("key: " + record.key() + " val: " + record.value() + " partition: " + partition + " offset: " + offset);
            }
        }
    }

    @Test
    public void consumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server01:9092,server02:9092,server03:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1");
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动提交时异步提交
//        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "");
//        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        List<String> topics = new ArrayList<>();
        topics.add("gentleduo-topic-1");
        consumer.subscribe(topics, new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("被移走的分区======>");
                Iterator<TopicPartition> iterator = collection.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("分配到的分区======>");
                Iterator<TopicPartition> iterator = collection.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> next = iterator.next();
                    int partition = next.partition();
                    long offset = next.offset();
                    System.out.println("key: " + next.key() + " val: " + next.value() + " partition: " + partition + " offset: " + offset);
                }
            }
        }
    }
}
