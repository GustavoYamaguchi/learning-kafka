package com.learning.kafka;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(getProperties());
        for (var i = 0; i < 100; i++) {
            var key = UUID.randomUUID().toString();
            var value = "1,2135,550";
            var record = new ProducerRecord("ECOMMERCE_NEW_ORDER", key, value);
            producer.send(record, (data, e) -> {
                if (e != null) {
                    System.out.println("Crashed while sending record.");
                } else {
                    System.out.println("Offset: "+ data.offset());
                    System.out.println("Partition: " + data.partition());
                }
            }).get();
        }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
