package com.kafka.streamsapp.fun_with_streams.streams_aggregation;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class AggregationStreamsExample {

    public static void main(String[] args) {
        String inTopic = "topic_in_agg";
        String outTopic = "topic_out_agg";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "6100");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "2000");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "70");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "120");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "5310");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "5800");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "1890");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "3120");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "2700");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "8900");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "1200");


        aggregateStreams(inTopic, outTopic);
    }

    private static void aggregateStreams(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> kTable = kStream.groupByKey().aggregate(
                () -> 0L,
                (key, value, newValue) -> Long.parseLong(value) + newValue,
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        kTable.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
