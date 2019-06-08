package com.kafka.streamsapp.fun_with_streams.banking_application;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class BankTransactionProducer {

    /*

     bin/kafka-console-consumer.sh\
     --bootstrap-server localhost:9092\
     --formatter kafka.tools.DefaultMessageFormatter\
     --property print.key=true\
     --property print.value=true\
     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
     --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
     --from-beginning\
     --topic bank_transactions_1

     bin/kafka-console-consumer.sh\
     --bootstrap-server localhost:9092\
     --formatter kafka.tools.DefaultMessageFormatter\
     --property print.key=true\
     --property print.value=true\
     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
     --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
     --from-beginning\
     --topic bank_balance_1

     */

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producing Batch : " + i);
            try {
                producer.send(newTransactionRecord("prabal"));
                Thread.sleep(100);
                producer.send(newTransactionRecord("arpit"));
                Thread.sleep(100);
                producer.send(newTransactionRecord("saharsh"));
                Thread.sleep(100);
                i++;
            } catch (InterruptedException e) {
                break;
            }
//            producer.close();
        }

    }

    private static ProducerRecord<String, String> newTransactionRecord(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant instant = Instant.now();
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", instant.toString());
        return new ProducerRecord<>("bank_transactions_2", name, transaction.toString());

    }
}
