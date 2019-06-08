# To write into a kafka topic along with KEY
 kafka-console-producer.sh \
 --broker-list localhost:9092 \ 
 --topic topic_name \
 --property parse.key=true\
 --property key.separator=:
 key 1, message 1
 key 2, message 2
 null, message 3
 
 #To read from a Kafka topic along with  KEY
 kafka-console-consumer.sh \
 --bootstrap-server localhost:9092\
 --topic topic_name\
 --formatter kafka.tools.DefaultMessageFormatter\
 --property print.key=true\
 --property print.value=true\
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\
 --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer 
 
