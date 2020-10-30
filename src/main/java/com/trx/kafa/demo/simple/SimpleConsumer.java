package com.trx.kafa.demo.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {

        Properties pros= new Properties();
        pros.put("bootstrap.servers","192.168.124.44:9093");
        pros.put("group.id","gp-test-group");
        pros.put("enable.auto.commit","true");
        pros.put("auto.commit.interval.ms","1000");
        pros.put("auto.offset.reset","earliest");
        pros.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        pros.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(pros);
        consumer.subscribe(Arrays.asList("gptest3"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d ,key =%s, value= %s, partition= %s%n", record.offset(), record.key(), record.value(), record.partition());
                }
            }
        }catch (Exception e){


        }finally{
consumer.close();
        }
    }
}
