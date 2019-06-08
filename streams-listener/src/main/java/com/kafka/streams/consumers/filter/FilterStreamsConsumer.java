package com.kafka.streams.consumers.filter;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FilterStreamsConsumer {


    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.consume("filtered_topic",
                "org.apache.kafka.common.serialization.IntegerDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }

    private static void unfilteredTopic() {

    }
}
