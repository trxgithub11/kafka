package com.trx.kafa.demo.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {


    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put("bootstrap.servers","192.168.124.44:9093");
        pros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        pros.put("acks","1");
        pros.put("retries",3);
        pros.put("batch.size",16384);
        pros.put("linger.ms",5);
        pros.put("buffer.memory",33554432);
        pros.put("max.block.ms",30000000);

        Producer<String,String> producer = new KafkaProducer<String,String>(pros);


        for (int i = 0; i <10 ; i++) {
            producer.send(new ProducerRecord<String, String>("gptest3",Integer.toString(i),Integer.toString(i)));

        }
        producer.close();
    }
}
