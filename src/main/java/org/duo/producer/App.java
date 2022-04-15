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

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        // 每当多个记录被发送到同一个分区时，生产者将尝试将记录一起批处理成更少的请求。 这有助于客户端和服务器的性能。 此配置控制默认批量大小（以字节为单位）。
        // 不会尝试批量处理大于此大小的记录。 发送到代理的请求将包含多个批次，每个分区都有一个可发送的数据。
        // 小批量会使批处理不太常见，并且可能会降低吞吐量（批量大小为零将完全禁用批处理）。
        // 一个非常大的批处理大小可能会更浪费内存，因为我们总是会分配一个指定批处理大小的缓冲区以预期额外的记录。
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); //16K
        // 生产者可用于缓冲等待发送到服务器的记录的内存总字节数。 如果记录的发送速度超过了它们可以传递到服务器的速度，则生产者将阻塞 <code>max.block.ms</code> 之后它将引发异常。
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");//32M
        // 配置控制<code>KafkaProducer.send()</code>和<code>KafkaProducer.partitionsFor()</code>将阻塞多长时间。
        // 这些方法可以因为缓冲区已满或元数据不可用而被阻塞。阻塞在用户提供的序列化程序或分区程序将不计入此超时。
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");//60秒
        // 等待批量数据的时间，超过此时间，未达到批量发送基本单位也发送。
        // （Kafka客户端积累一定量的消息后会统一组装成一个批量消息发出，满足batch.size设置值或达到linger.ms超时时间都会发送）
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        // 请求的最大大小（以字节为单位）。 此设置将限制生产者在单个请求中发送的记录批次数，以避免发送大量请求。 这也有效地限制了最大记录批量大小。
        // 请注意，服务器对记录批量大小有自己的上限，可能与此不同。
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        // 在阻塞之前，客户端将在单个连接上发送的最大未确认请求数。请注意，如果此设置设置为大于1并且发送失败，则存在由于重试（即启用重试）而导致消息重新排序的风险。
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // 内核中tcp的发送缓冲区大小，如果设置为-1则是按照linux的默认值：cat /proc/sys/net/core/wmem_max
        properties.setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "32768");
        // 内核中tcp的接收缓冲区大小，如果设置为-1则是按照linux的默认值：cat /proc/sys/net/core/rmem_max
        properties.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 现在的producer就是一个提供者，面向的其实是broker，虽然在使用的时候我们期望把数据打入topic
        String topic = "gentleduo-topic-1";

        while (true) {
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
    }

    @Test
    public void consumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server01:9092,server02:9092,server03:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-0");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

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

//        Map<TopicPartition, Long> tts = new HashMap<>();
//        // 通过consumer取回自己的分区
//        Set<TopicPartition> as = consumer.assignment();
//        while (as.size()==0) {
//            consumer.poll(Duration.ofMillis(100));
//            as = consumer.assignment();
//        }
//        // 自己填充一个hashmap，为每个分区设置对应的时间戳
//        for (TopicPartition partition : as) {
//            tts.put(partition, System.currentTimeMillis() - 1 * 1000);
//        }
//        //通过consumer的api，取回timeindex的数据
//        Map<TopicPartition, OffsetAndTimestamp> offtime = consumer.offsetsForTimes(tts);
//        for (TopicPartition partition : as) {
//            // 通过consumer的seek方法，修正offset
//            OffsetAndTimestamp offsetAndTimestamp = offtime.get(partition);
//            long offset = offsetAndTimestamp.offset();
//            consumer.seek(partition, offset);
//        }

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
