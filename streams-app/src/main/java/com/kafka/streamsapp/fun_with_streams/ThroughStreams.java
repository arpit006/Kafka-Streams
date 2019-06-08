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
public class ThroughStreams {

    public static void main(String[] args) {
        String inTopic = "sub_topic";

        String betTopic = "mid_through_topic";

        String outTopic = "through_out_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(betTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "one");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "two");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "three");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "four");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "five");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "six");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "seven");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "eight");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "nine");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "ten");

        throughStreams(inTopic, betTopic, outTopic);
    }

    private static void throughStreams(String inTopic, String betTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> throughStream = kStream.through(betTopic, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, Integer> integerKStream = throughStream.map((k, v) -> KeyValue.pair(v, v.length()));

        integerKStream.to(outTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);

    }
}
