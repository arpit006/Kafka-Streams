package com.kafka.streamsapp.fun_with_streams;

import com.kafka.streamsapp.config.KafkaStreamsHandler;
import com.kafka.streamsapp.parser.JsonParser;
import com.kafka.streamsapp.producer.ProducerKafka;
import com.kafka.streamsapp.reader.FileReader;
import com.kafka.streamsapp.topic.TopicManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Map;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class TopicsStreams {

    private static final Logger _logger = LoggerFactory.getLogger(TopicsStreams.class);

    public static void main(String[] args) {
        String inTopic = "topic_100";
        String outTopic = "topic_101";

        TopicManager.createTopic(inTopic, 3, (short) 1);
        TopicManager.createTopic(outTopic, 3, (short) 1);

        String inJson = FileReader.readFileAsString("src/main/resources/Details.json");

        Map<String, Object> stringObjectMap = JsonParser.parseJson(inJson);

        ProducerKafka.postToKafkaTopic(inTopic, String.valueOf(stringObjectMap.get("id")), inJson);

        streamFromOneTopicToAnother(inTopic, outTopic);

    }

    /**
     * This method streams from one topic to another topic.
     *
     * @param inTopic
     */
    private static void streamFromOneTopicToAnother(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(inTopic, Consumed.with(Serdes.String(), Serdes.String()));
        kStream.to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreamsHandler.kafkaStreams(builder, inTopic);
    }


    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "topic_101", partitionOffsets = {
                    @PartitionOffset(partition = "0", initialOffset = "0"),
                    @PartitionOffset(partition = "1", initialOffset = "0"),
                    @PartitionOffset(partition = "2", initialOffset = "0")
            })
    })
    public static void listen(@Payload String inJson,
                              @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                              @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionId,
                              @Header(KafkaHeaders.OFFSET) String offset,
                              @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        _logger.info("#### Listening to Kafka Topic : " + topic +
                "\nData : " + inJson +
                "\nPartition : " + partitionId +
                "\nKey : " + key +
                "\nOffset : " + offset);
    }
}
