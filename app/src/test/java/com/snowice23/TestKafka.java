package com.snowice23;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

import static com.snowice23.ConnectionHelper.KAFKA_TOPIC;

public class TestKafka {

    public static void main(String[] args) throws InterruptedException {

        Properties props = ConnectionHelper.getKafkaProducerProperties();

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {
            Thread.sleep(500);

            // random numbers from [0, 10>
            Integer value = random.nextInt(10);

            //System.out.println("Producing: " + value);

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(KAFKA_TOPIC, String.valueOf(value));

            // send data - asynchronous
            producer.send(producerRecord);

            // flush data - synchronous
            producer.flush();
        }
    }
}
