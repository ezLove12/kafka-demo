package io.viethm1.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        //log info
        log.info("Creating producer with callback");
        //create Producer properties
        Properties properties = new Properties();
        //connecting to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "demo_java";
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "hello " + i;
                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //sent data with callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //execute everytime a record successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Key " + key + "|" +
                                    "Partition " + recordMetadata.partition() + "\n");
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
