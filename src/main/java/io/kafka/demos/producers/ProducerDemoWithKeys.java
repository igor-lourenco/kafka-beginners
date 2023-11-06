package io.kafka.demos.producers;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka ProducerDemoWithKeys!");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(getPropertiesKafka());

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "topic_demo_java";
                String key = "id_" + i;
                String value = "Hello topic_demo_java " + i;

                //create a Producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    //executes every time a record successfully send or an exception is thrown
                    if (e == null) {
                        //the record was successfully sent
                        log.info("Key: " + key + " -> " + "Value: " + value + " | " +
                                "Partition: " + recordMetadata.partition());
                    } else {
                        log.error("Error while producing ", e);
                    }
                });
            }

            log.info("=====================================================");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }

    private static Properties getPropertiesKafka(){
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }
}
