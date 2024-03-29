package com.snowice23.snowpipe;

import com.snowice23.ConnectionHelper;
import net.snowflake.ingest.streaming.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.snowice23.ConnectionHelper.KAFKA_TOPIC;
import static java.lang.System.out;


// https://github.com/snowflakedb/snowflake-ingest-java/blob/master/src/main/java/net/snowflake/ingest/streaming/example/SnowflakeStreamingIngestExample.java
public class Ingester {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.putAll(ConnectionHelper.getKafkaConsumerProperties("ingestor-group"));
        props.putAll(ConnectionHelper.getProperties());

        // Create a streaming ingest client
        try (SnowflakeStreamingIngestClient client =
                     SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build()) {

            // Create an open channel request on table MY_TABLE, note that the corresponding
            // db/schema/table needs to be present
            // Example: create or replace table MY_TABLE(c1 number);
            OpenChannelRequest request1 =
                    OpenChannelRequest.builder("MY_CHANNEL")
                            .setDBName("STREAMING_DEMO")
                            .setSchemaName("PUBLIC")
                            .setTableName("EVENTS")
                            .setOnErrorOption(
                                    OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                            .build();

            // Open a streaming ingest channel from the given client
            SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

            out.println("Opened SnowPipe channel: " + channel1.getName());

            // Kafka Consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(KAFKA_TOPIC));

            out.println("Subscribed to Kafka topic: " + consumer.subscription());

            // Insert events into the snowpipe streaming channel (Using insertRows API)
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                //out.println("Listening...");
                for (ConsumerRecord<String, String> record : records) {
                    out.println("Ingesting:" + record.value());

                    String value = record.value();

                    Map<String, Object> row = new HashMap<>();

                    // c1 corresponds to the column name in table
                    row.put("value", Integer.valueOf(value));

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    row.put("timestamp", timestamp.toInstant());

                    // Insert the row with the current offset_token
                    InsertValidationResponse response = channel1.insertRow(row, null);
                    if (response.hasErrors()) {
                        // Simply throw if there is an exception, or you can do whatever you want with the
                        // erroneous row
                        throw response.getInsertErrors().get(0).getException();
                    }

                }


                //channel1.close().get();

            }
        }

    }
}
