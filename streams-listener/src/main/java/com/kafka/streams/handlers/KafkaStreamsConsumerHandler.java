package com.kafka.streams.handlers;

import com.kafka.streams.consumers.TopicsStreams;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class KafkaStreamsConsumerHandler {

    private static final Logger _logger = LoggerFactory.getLogger(TopicsStreams.class);

    public static <K, V> void consume(String inTopic, String keyDeserializer, String valueDeserializer) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, inTopic);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, inTopic);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singleton(inTopic));

        while (true) {
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            consumerRecords.forEach(consumerRecord ->
                    _logger.info("#### Consuming Data from -->" +
                            "\nTopic :      " + consumerRecord.topic() +
                            "\nPartition :  " + consumerRecord.partition() +
                            "\nOffset :     " + consumerRecord.offset() +
                            "\nKey :        " + consumerRecord.key() +
                            "\nValue :      " + consumerRecord.value()
                    )
            );
//            kafkaConsumer.commitAsync();
        }
    }
}
