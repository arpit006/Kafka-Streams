package com.kafka.streamsapp.fun_with_streams.streams_aggregation;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class AggregationTableExample {

    public static void main(String[] args) {
        String inTopic = "table_in_agg_topic";
        String outTopic = "table_out_agg_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "6100");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "70");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "2000");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "120");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "5310");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "1890");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "5800");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "3120");
        ProducerKafka.postToKafkaTopic(inTopic, "saharsh", "2700");
        ProducerKafka.postToKafkaTopic(inTopic, "prabal", "1200");
        ProducerKafka.postToKafkaTopic(inTopic, "arpit", "8900");

        aggregateTable(inTopic, outTopic);
    }

    private static void aggregateTable(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> kTable = builder.table(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedTable<String, String> kGroupedTable = kTable.groupBy(KeyValue::pair);

        KTable<String, Long> aggregateTable = kGroupedTable.aggregate(
                () -> 0L,
                (key, value, aggregate) -> Long.parseLong(value) + aggregate,
                (key, value, aggregate) -> aggregate - Long.parseLong(value),
                Materialized.with(Serdes.String(), Serdes.Long())

        );

        aggregateTable.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
