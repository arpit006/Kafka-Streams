package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class GroupedTable {

    public static void main(String[] args) {
        String inTopic = "grouped_table_topic_22";
        String tableTopic = "table_group_topic_22";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(tableTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "3", "himanshu");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "pooja");
        ProducerKafka.postToKafkaTopic(inTopic, "1", "arpit");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "shivansh");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "prabal");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "saharsh");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "diksha");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "manas");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "kansal");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "radhika");
        ProducerKafka.postToKafkaTopic(inTopic, "11", "manas");
        ProducerKafka.postToKafkaTopic(inTopic, "12", "malik");

        groupedTables(inTopic, tableTopic);
    }

    private static void groupedTables(String inTopic, String tableTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> kTable = builder.table(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedTable<String, String> kGroupedTable = kTable.groupBy((k, v) -> KeyValue.pair(String.valueOf(v.length()), v));

        KTable<String, Long> tableCount = kGroupedTable.count();

        tableCount.toStream().to(tableTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
