package io.viethm1.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        //log info
        log.info("Creating producer");
        //create Producer properties
        Properties properties = new Properties();
        //connecting to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
        //sent data
        producer.send(producerRecord);
        //tell producer to send all data and block until done --> synchronous
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
