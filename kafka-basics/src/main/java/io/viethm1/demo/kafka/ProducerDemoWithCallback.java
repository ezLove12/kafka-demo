package io.viethm1.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        //log info
        log.info("Creating producer with callback");
        //create Producer properties
        Properties properties = new Properties();
        //connecting to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //set batch size
        properties.setProperty("batch.size", "400");

        //set round robin partitioner -> not recommend in production
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //sent multiple record
        for (int j = 0; j < 10; j++) {


            for (int i = 0; i < 30; i++) {
                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);
                //sent data with callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //execute everytime a record successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic " + recordMetadata.topic() + "\n" +
                                    "Partition " + recordMetadata.partition() + "\n" +
                                    "Offset " + recordMetadata.offset() + "\n" +
                                    "Timestamp" + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //tell producer to send all data and block until done --> synchronous
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
