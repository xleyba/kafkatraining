package net.javier.kafkatraining.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemowithThreads {

    public static void main(String[] args) {
        new ConsumerDemowithThreads().run();
    }

    private ConsumerDemowithThreads() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemowithThreads.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "javier";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted: " + e.getMessage());
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {

            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {

            try {

                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                    System.out.println("Records: " + Integer.toString(records.count()));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("-- Value: " + record.value() + System.lineSeparator() +
                                "--- Key: " + record.key() + System.lineSeparator() +
                                "-- Partition: " + record.partition() + System.lineSeparator());

                    }
                }
            } catch(WakeupException we) {
                this.logger.info("Receive shutdown ");
            } finally {
                this.consumer.close();
                this.latch.countDown();
            }
        }

        public void shutdown() {
            this.consumer.wakeup();
        }
    }
}
