package com.kafka.streams.consumers.branches;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class Branch3StreamConsumer {
    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("b3_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
    }
}
