package com.kafka.streams.consumers.aggregations;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class AggregationTableConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.consume("table_out_agg_topic",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.LongDeserializer"
                );
    }
}
