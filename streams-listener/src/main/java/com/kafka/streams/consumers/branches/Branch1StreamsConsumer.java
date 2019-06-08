package com.kafka.streams.consumers.branches;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class Branch1StreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("b1_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
                );
    }
}
