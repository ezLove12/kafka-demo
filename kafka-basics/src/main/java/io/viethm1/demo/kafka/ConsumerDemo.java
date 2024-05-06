package io.viethm1.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    private static final String group_id = "my-java-application";
    private static final String topic = "demo_java";
    public static void main(String[] args) {
        //log info
        log.info("Creating consumer");
        //create Producer properties
        Properties properties = new Properties();
        //connecting to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", group_id);
        //set offset value: none=must create consumer group first, earliest , latest= --from-beginning
        properties.setProperty("auto.offset.reset", "earliest");
        //creates a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(List.of(topic));

        //poll for data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record: records){
                log.info("key: {} value: {}", record.key(), record.value());
                log.info("partition: {} offset: {}", record.partition(), record.offset());
            }
        }
    }
}
