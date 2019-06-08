package com.kafka.streams.consumers.groupedStreamsAndTable;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class GroupTableConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.consume("table_group_topic_22",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer"
                );
    }
}
