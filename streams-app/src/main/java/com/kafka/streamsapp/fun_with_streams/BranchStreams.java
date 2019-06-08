package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class BranchStreams {

    public static void main(String[] args) {
        String inTopic = "main_topic";
        String b1Topic = "b1_topic";
        String b2Topic = "b2_topic";
        String b3Topic = "b3_topic";
        String b4Topic = "b4_topic";

        TopicManager.createTopic(inTopic, 3, 1);
        TopicManager.createTopic(b1Topic, 3, 1);
        TopicManager.createTopic(b2Topic, 3, 1);
        TopicManager.createTopic(b3Topic, 3, 1);
        TopicManager.createTopic(b4Topic, 3, 1);

        ProducerKafka.postToKafkaTopic(inTopic, "1", "arpit");
        ProducerKafka.postToKafkaTopic(inTopic, "2", "pooja");
        ProducerKafka.postToKafkaTopic(inTopic, "3", "shivansh");
        ProducerKafka.postToKafkaTopic(inTopic, "4", "prabal");
        ProducerKafka.postToKafkaTopic(inTopic, "5", "himanshu");
        ProducerKafka.postToKafkaTopic(inTopic, "6", "radhika");
        ProducerKafka.postToKafkaTopic(inTopic, "7", "kansal");
        ProducerKafka.postToKafkaTopic(inTopic, "8", "shivam");
        ProducerKafka.postToKafkaTopic(inTopic, "9", "diksha");
        ProducerKafka.postToKafkaTopic(inTopic, "10", "saharsh");
        ProducerKafka.postToKafkaTopic(inTopic, "11", "manas");
        ProducerKafka.postToKafkaTopic(inTopic, "12", "mrigank");
        ProducerKafka.postToKafkaTopic(inTopic, "13", "ashu");
        ProducerKafka.postToKafkaTopic(inTopic, "14", "mohith");
        ProducerKafka.postToKafkaTopic(inTopic, "15", "kalyan");
        ProducerKafka.postToKafkaTopic(inTopic, "16", "rahul");
        ProducerKafka.postToKafkaTopic(inTopic, "17", "hero");
        ProducerKafka.postToKafkaTopic(inTopic, "18", "rithik");

        branchTheStream(inTopic, b1Topic, b2Topic, b3Topic, b4Topic);
    }

    private static void branchTheStream(String inTopic, String b1, String b2, String b3, String b4) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String>[] branches = kStream.branch(
                (k, v) -> v.startsWith("a"),
                (k, v) -> v.startsWith("p"),
                (k, v) -> v.startsWith("m"),
                (k, v) -> true);

        branches[0].to(b1, Produced.with(Serdes.String(), Serdes.String()));
        branches[1].to(b2, Produced.with(Serdes.String(), Serdes.String()));
        branches[2].to(b3, Produced.with(Serdes.String(), Serdes.String()));
        branches[3].to(b4, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }
}
