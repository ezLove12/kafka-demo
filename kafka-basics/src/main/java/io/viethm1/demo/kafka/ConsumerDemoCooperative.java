package io.viethm1.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
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
        //set offset value: none=must create consumer group first, earliest = --from-beginning , latest
        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //Strategy for static assignment
//        properties.setProperty("group.instance.id", "");
        //creates a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to a main thread
        final Thread mainThread = Thread.currentThread();

        //add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(List.of(topic));

            //poll for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: {} value: {}", record.key(), record.value());
                    log.info("partition: {} offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch (Exception e){
            log.error("Unexpected exception in the consumer "+e);
        } finally {
            //close the consumer, commit the offset
            consumer.close();
            log.info("Consumer is shutdown");
        }
    }
}
