package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FilterAndNotFilterStreams {

    public static void main(String[] args) {
        String inTopic = "unfiltered_topic";
        String filterTopic = "filtered_topic";
        String notFilteredTopic = "not_filtered_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(filterTopic, 3, 1);
        TopicManager.createTopic(notFilteredTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "a");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "ab");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "abc");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "abcd");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "abcde");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "abcdef");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "abcdefg");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "abcdefgh");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "abcdefghi");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "abcdefghij");

        playWithFilterStreams(inTopic, filterTopic, notFilteredTopic);
    }

    private static void playWithFilterStreams(String inTopic, String filteredTopic, String notFilteredTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<Integer, String> filteredStream = kStream
                .filter((k, v) -> v.length() > 5)
                .map((k, v) -> KeyValue.pair(v.length(), v));

        KStream<Integer, String> nonFilteredStream = kStream
                .filterNot((k, v) -> v.length() > 5)
                .map((k, v) -> KeyValue.pair(v.length(), v));

        filteredStream.to(filteredTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        nonFilteredStream.to(notFilteredTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
