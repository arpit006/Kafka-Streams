package com.kafka.streams.consumers;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class ThroughBetweenStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("mid_through_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
    }
}
