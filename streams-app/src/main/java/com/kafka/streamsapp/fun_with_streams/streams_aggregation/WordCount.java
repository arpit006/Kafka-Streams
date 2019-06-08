package com.kafka.streamsapp.fun_with_streams.streams_aggregation;


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

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class WordCount {

    public static void main(String[] args) {
        String inTopic = "aggregate_in_topic_1";
        String outTopic = "aggregate_out_topic_1";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "This is to test");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "to be the best");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "test is kafka");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "kafka is distributed");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "kafka is arpit");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "arpit is kafka");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "This tutorial is best");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "arpit is this tutorial");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "Learn it easy way");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "find way to be best");
        ProducerKafka.postToKafkaTopic(inTopic, "11", "Be like Arpit");

        aggregateMethodCount(inTopic, outTopic);
    }

    private static void aggregateMethodCount(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> wordCountStream = kStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(v -> Arrays.asList(v.split("\\W+")))
                .selectKey((k, v) -> v);

        KGroupedStream<String, String> kGroupedStream = wordCountStream.groupByKey();

        KTable<String, Long> kTable = kGroupedStream.count();

        kTable.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
