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

import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class MapAndFlatMapStreams {

    public static void main(String[] args) {
        String inTopic = "uni_topic";
        String mapTopic = "topic_map_out";
        String flatMapTopic = "topic_flatmap_out";

        TopicManager.createTopic(inTopic, 3,1);
        TopicManager.createTopic(mapTopic, 3,1);
        TopicManager.createTopic(flatMapTopic, 3,1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "I");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "love");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "Kafka");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "the");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "most");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "in");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "the");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "distributed");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "systems");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "Arpit");

        mapAndFlatMapStreams(inTopic, mapTopic, flatMapTopic);
    }

    private static void mapAndFlatMapStreams(String inTopic, String mapTopic, String flatMapTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Integer> transformedMapStream = kStream.map((k,v) -> KeyValue.pair(v, v.length()));

        KStream<String, Integer> transformedFlatMapStream = kStream.flatMap((k,v) -> {
            List<KeyValue<String, Integer>> list = new LinkedList<>();
            list.add(KeyValue.pair(v.toLowerCase(), v.length() * 10));
            list.add(KeyValue.pair(v.toUpperCase(), v.length() * 100));
            return list;
        });

        transformedMapStream.to(mapTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        transformedFlatMapStream.to(flatMapTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
