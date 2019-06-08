package com.kafka.streams.consumers.filter;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class NotFilteredStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.consume("not_filtered_topic",
                "org.apache.kafka.common.serialization.IntegerDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
