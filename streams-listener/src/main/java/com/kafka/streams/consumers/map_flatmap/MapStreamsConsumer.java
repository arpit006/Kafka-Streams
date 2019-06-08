package com.kafka.streams.consumers.map_flatmap;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class MapStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, Integer>consume("topic_map_out",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");
    }
}
