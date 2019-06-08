package com.kafka.streams.consumers.groupedStreamsAndTable;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class GroupedStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.consume("grouped_stream_topic_2",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer"
        );
    }
}
