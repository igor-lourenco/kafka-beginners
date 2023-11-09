package io.kafka.demos.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoThreads.class.getSimpleName());

    public static void main(String[] args)  {
        ConsumerDemoWorker consumerDemoWorker = new ConsumerDemoWorker();

        //  The Thread instance is created with consumerDemoWorker passed as an argument.
        //  This means that the logic contained in ConsumerDemoWorker will be executed in a new, separate thread
        new Thread(consumerDemoWorker).start();

        //Register a "closing hook." Specific action when the program is about to terminate. In this case,
        // a new Thread instance is created, where the ConsumerDemoCloser class is used to controllably close
        // a ConsumerDemoWorker instance that is running in a separate thread.
        Runtime.getRuntime().addShutdownHook(new Thread(new ConsumerDemoCloser(consumerDemoWorker)));
    }

    private static class ConsumerDemoCloser implements Runnable {
        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCloser.class);

        private final ConsumerDemoWorker consumerDemoWorker;

        ConsumerDemoCloser(final ConsumerDemoWorker consumerDemoWorker) {
            this.consumerDemoWorker = consumerDemoWorker;
        }

        @Override
        public void run() { //method that is called when the program is about to exit.
            try {
                log.info("Iniciando classe ConsumerDemoCloser...");
                consumerDemoWorker.shutdown();
            } catch (InterruptedException e) {
                log.error("Error shutting down consumer", e);
            }
        }
    }

    private static class ConsumerDemoWorker implements Runnable {
        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWorker.class);

        private CountDownLatch countDownLatch;
        private Consumer<String, String> consumer;

        @Override
        public void run() { //method that is called when this class is started
            log.info("Iniciando classe ConsumerDemoWorker...");
            countDownLatch = new CountDownLatch(1);
            consumer = new KafkaConsumer<>(getPropertiesKafka());
            consumer.subscribe(Collections.singleton("topic_demo_java"));

            final Duration pollTimeout = Duration.ofMillis(1000);

            try {
                while (true) {
                    log.info("Polling");
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);

                    for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        log.info("Getting consumer record key: '" + consumerRecord.key() + "', value: '" +
                                consumerRecord.value() + "', partition: " + consumerRecord.partition() +
                                " and offset: " + consumerRecord.offset() + " at " + new Date(consumerRecord.timestamp()));
                    }
                }
            } catch (WakeupException e) {// exception thrown by consumer.wakeup()
                log.info("Consumer poll woke up");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        void shutdown() throws InterruptedException {
            consumer.wakeup(); // is used to interrupt the consumer consumption cycle asynchronously by throwing WakeupException exception
            countDownLatch.await();
            log.info("Consumer closed");
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
