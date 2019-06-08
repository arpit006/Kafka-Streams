package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class ForEachStreams {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForEachStreams.class);

    public static void main(String[] args) {
        String inTopic = "for_each_topic";

        TopicManager.createTopic(inTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "asta-lavista");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "shoe-badam-pista");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "kafka-is-best");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "best-distributed-system");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "system-above-par");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "i-love-kafka");

        forEachStreams(inTopic);
    }

    private static void forEachStreams(String inTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        kStream.foreach((k, v) -> System.out.println("Key -> " + k + " : Value -> " + v));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);

    }
}
