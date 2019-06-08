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

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class GroupedStreams {

    public static void main(String[] args) {
        String inTopic1 = "grouped_topic_21";
        String streamTopic = "grouped_stream_topic_2";

        TopicManager.createTopic(inTopic1, 3, 1);
        TopicManager.createTopic(streamTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic1, "3", "himanshu");
        ProducerKafka.postToKafkaTopic(inTopic1, "1", "arpit");
        ProducerKafka.postToKafkaTopic(inTopic1, "2", "pooja");
        ProducerKafka.postToKafkaTopic(inTopic1, "4", "shivansh");
        ProducerKafka.postToKafkaTopic(inTopic1, "5", "prabal");
        ProducerKafka.postToKafkaTopic(inTopic1, "6", "saharsh");
        ProducerKafka.postToKafkaTopic(inTopic1, "7", "manas");
        ProducerKafka.postToKafkaTopic(inTopic1, "8", "diksha");
        ProducerKafka.postToKafkaTopic(inTopic1, "9", "kansal");
        ProducerKafka.postToKafkaTopic(inTopic1, "10", "radhika");
        ProducerKafka.postToKafkaTopic(inTopic1, "12", "malik");
        ProducerKafka.postToKafkaTopic(inTopic1, "11", "manas");

        groupedStreams(inTopic1, streamTopic);
    }

    private static void groupedStreams(String inTopic, String streamTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedStream<String, String> kGroupedStream = kStream
                .groupBy((k, v) -> String.valueOf(v.length()));

        KTable<String, Long> streamCount = kGroupedStream.count();

        streamCount.toStream().to(streamTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);

    }
}
