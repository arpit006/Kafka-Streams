package com.kafka.streams.consumers;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class TopicsStreams {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("topic_101",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
