package com.snowice23.streaming;

import java.sql.*;
import java.time.Duration;
import java.util.*;

import com.snowice23.ConnectionHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static com.snowice23.ConnectionHelper.KAFKA_TOPIC;
import static java.lang.System.*;

public class Alerter {
    public static void main(String[] args) throws Exception {

        Connection conn = ConnectionHelper.getConnection();
        String insertAlert = "INSERT INTO alerts VALUES(?, ?, ?)";

        // create statement
        PreparedStatement statement = conn.prepareStatement(insertAlert);

        Properties props = new Properties();
        props.putAll(ConnectionHelper.getKafkaConsumerProperties("alerter-group"));
        props.putAll(ConnectionHelper.getProperties());

        // Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(KAFKA_TOPIC));

        out.println("Subscribed to Kafka topic: " + consumer.subscription());

        // Insert alerts into the database with JDBC
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            //out.println("Listening...");
            for (ConsumerRecord<String, String> record : records) {

                int value = Integer.parseInt(record.value());

                // implement simple stream analyzer to simulate outlier detection
                if (value >= 8) {

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    // insert a row
                    statement.setTimestamp(1, timestamp);
                    statement.setInt(2, value);
                    statement.setString(3, "alert raised");

                    statement.executeUpdate();

                    out.println("Alert:" + record.value());
                }
            }

        }

    }
}
