package com.kafka.streams.consumers;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class LengthOfEachStringStreamConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, Integer>consume("string_length_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");
    }
}
