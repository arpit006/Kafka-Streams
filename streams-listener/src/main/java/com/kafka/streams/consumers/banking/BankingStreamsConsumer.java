package com.kafka.streams.consumers.banking;

import com.kafka.streams.handlers.KafkaStreamsConsumerHandler;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class BankingStreamsConsumer {

    public static void main(String[] args) {
        KafkaStreamsConsumerHandler.<String, String>consume("bank_balance_2",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
    }
}
