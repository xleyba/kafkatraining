package net.javier.kafkatraining.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithKeys {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemowithKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";

        for (int x = 0; x < 10; x++) {

            String value = "Mi mensaje " + x;
            String key = "id_" + x;

            ProducerRecord pr = new ProducerRecord<>(topic, key, value);

            System.out.println("Sending...");
            try {
                producer.send(pr, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error(e.getMessage());
                        } else {
                            logger.info("Produced metadata: " + recordMetadata.topic() + System.lineSeparator() +
                                    "partition: " + recordMetadata.partition() + System.lineSeparator() +
                                    "offset: " + recordMetadata.offset());
                        }
                    }
                }).get();



            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        System.out.println("Flushing...");
        producer.flush();
        System.out.println("Closing...");
        producer.close();

/**
 Properties props = new Properties();
 props.put("bootstrap.servers", "localhost:9092");
 props.put("transactional.id", "my-transactional-id");
 Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

 producer.initTransactions();

 try {
 producer.beginTransaction();
 for (int i = 0; i < 100; i++)
 producer.send(new ProducerRecord<>("first_topic", Integer.toString(i), Integer.toString(i)));
 producer.commitTransaction();
 } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 // We can't recover from these exceptions, so our only option is to close the producer and exit.
 producer.close();
 } catch (KafkaException e) {
 // For all other exceptions, just abort the transaction and try again.
 producer.abortTransaction();
 }
 producer.close();

 */

    }

}
