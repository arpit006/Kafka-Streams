package com.kafka.streams.consumers;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * This class consumes from the intermediate topic where the streams traverse.
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class ThroughEndStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("through_out_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer"
        );
    }
}
