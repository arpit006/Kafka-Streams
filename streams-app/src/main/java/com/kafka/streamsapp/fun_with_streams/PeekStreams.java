package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class PeekStreams {

    public static void main(String[] args) {
        String inTopic = "peek_topic";

        TopicManager.createTopic(inTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "peek1");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "peek2");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "peek3");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "peek4");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "peek5");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "peek6");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "peek7");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "peek8");

        peekStreams(inTopic);

    }

    private static void peekStreams(String inTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        kStream.peek((k, v) -> System.out.println("Key -> " + k + " : Value -> " + v));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
