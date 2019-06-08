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
public class LengthOfEachStringStreams {


    public static void main(String[] args) {
        String inTopic = "string_topic";
        String outTopic = "string_length_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "This line is of 6 length");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "This line length 4");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "length 2");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "length is 3");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "This line length is 5");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "1");

        stringLengthStream(inTopic, outTopic);
    }

    private static void stringLengthStream(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Integer> stringStream = kStream
                .map((k, v) -> KeyValue.pair(v, v.length()));

        stringStream.to(outTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
