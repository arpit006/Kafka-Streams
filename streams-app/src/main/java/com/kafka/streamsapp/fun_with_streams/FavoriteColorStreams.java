package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class FavoriteColorStreams {

    public static void main(String[] args) {
        String inTopic = "color_fav_15";
        String betTopic = "color_fav_int_15";
        String outTopic = "color_fav_out_15";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(betTopic, 3, 1);
        TopicManager.createTopic(outTopic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "arpit,red");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "shivansh,blue");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "himanshu,green");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "prabal,red");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "mohith,blue");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "arpit,blue");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "shivansh,green");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "saharsh,violet");

        userAndFavoriteColor(inTopic, betTopic, outTopic);
    }

    /*

    kafka-topics --create --zookeeper localhost:2181 --topic color_fav_1 --replication-factor 1 --partitions 3

     bin/kafka-console-consumer.sh\
      --bootstrap-server localhost:9092\
      --topic color_fav_out_14\
      --formatter kafka.tools.DefaultMessageFormatter\
      --property print.key=true\
      --property print.value=true\
      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
      --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer\
      --from-beginning


     */
    private static void userAndFavoriteColor(String inTopic, String betTopic, String outTopic) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> userAndColor = kStream
                .filter((k, v) -> v.contains(","))
                .map((k, v) -> KeyValue.pair(v.split(",")[0], v.split(",")[1]))
                .filter((user, color) -> Arrays.asList("red", "blue", "green").contains(color));

        userAndColor.to(betTopic, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> userColorTable = builder.table(betTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> favColorUsers = userColorTable
                .groupBy((k, v) -> KeyValue.pair(v, v))
                .count();

        favColorUsers.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
