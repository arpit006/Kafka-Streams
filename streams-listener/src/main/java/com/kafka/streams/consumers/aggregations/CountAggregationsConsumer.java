package com.kafka.streams.consumers.aggregations;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class CountAggregationsConsumer {

    public static void main(String[] args) {

        KafkaStreamsConsumerHandler.<String, Long>consume("aggregate_out_topic_1",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer"
                );
    }
}
