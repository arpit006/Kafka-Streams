package com.kafka.streamsapp.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class KafkaStreamsHandler {

    private static Properties getProperty(String clientApplicationId) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, clientApplicationId);
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, clientApplicationId);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        properties.put(StreamsConfig.POLL_MS_CONFIG, 300);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public static void kafkaStreams(StreamsBuilder builder, String clientApplicationId) {
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperty(clientApplicationId));

        kafkaStreams.cleanUp();

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

}

