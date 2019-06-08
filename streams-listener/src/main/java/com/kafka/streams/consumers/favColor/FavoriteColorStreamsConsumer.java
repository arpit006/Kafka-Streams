package com.kafka.streams.consumers.favColor;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FavoriteColorStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, Long>consume("color_fav_out_15",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer");
    }
}
