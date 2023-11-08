package io.kafka.demos.consumers;

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

    public static void main(String[] args) {
        log.info("I am a kafka ConsumerDemoCooperative!");

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getPropertiesKafka());

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook, is snippet of code that will be automatically executed when the Java program is about to exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let´s exit by calling consumer.wakeup()...");
            consumer.wakeup(); //is used to break the consumer consumption loop asynchronously.

            //join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }finally {
                log.info("Thread finished!");
            }
        }));

        try {
            String topic = "topic_demo_java";
            //subscribe to a topic
            consumer.subscribe(List.of(topic));

            //poll for data
            while (true) {
                log.info("Polling");

                //returns list of records
                ConsumerRecords<String, String> recordsList = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : recordsList) {
                    log.info("Key: " + record.key() +
                            " | Value: " + record.value() +
                            " | Partition: " + record.partition() +
                            " | Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) { // exception thrown by consumer.wakeup()...
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer");
        } finally {
            consumer.close(); //close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }

    private static Properties getPropertiesKafka() {
        Properties properties = new Properties();
        String groupId = "my_java_application";

//         connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

//        set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

//        rebalance strategy is identical to StickyAssignor but supports cooperative rebalances and therefore
//         consumers can keep on consuming from the topic
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
//        properties.setProperty("group.instance.id", "...."); //strategy for static assignments

//        consumer starts reading messages from the beginning of the topic
        properties.setProperty("auto.offset.reset", "earliest");

        return properties;
    }
}
