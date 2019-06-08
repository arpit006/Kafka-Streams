package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class WordCount {


    public static void main(String[] args) {
        String inTopic = "word_count_in_topic";
        String outTopic = "word_count_out_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "This is the best place in the world");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "Kafka is the best distributed system");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "Kafka is way more than just a Queueing System");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "Kafka has the best implementation for asynchronous systems");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "I Love Kafka");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "My name is Arpit");

        wordCountStreams(inTopic, outTopic);

    }

    private static void wordCountStreams(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KStream<String, String> wordsStream = kStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(v -> Arrays.asList(pattern.split(v)))
                .selectKey((k, v) -> v);

        KGroupedStream<String, String> kGroupedStream = wordsStream.groupByKey();

        KTable<String, Long> kTable = kGroupedStream.count();

        kTable.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);

    }
}
