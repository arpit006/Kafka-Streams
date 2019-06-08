package com.kafka.streams.consumers.favColor;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FavoriteColorIntermediateTopicConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("user_color_topic_2",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
