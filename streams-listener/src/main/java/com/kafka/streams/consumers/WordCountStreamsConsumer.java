package com.kafka.streams.consumers;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class WordCountStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, Long>consume("word_count_out_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer");
    }
}
