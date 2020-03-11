package com.training.kafka.producer;



import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Properties;


@Slf4j
@RunWith(SpringRunner.class)
public class KafkaProducerTest {

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void test() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks","all");
        props.put("retries","3");
        props.put("batch.size","16384");
        props.put(ProducerConfig.LINGER_MS_CONFIG,"1"); //waiting time
        props.put("buffer.memory","33554432"); //RecordAccumulator buffer size, 32 MB
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(props);
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "FIrst test Value " + i + "----->");
//            kafkaProducer.send(producerRecord);
            kafkaProducer.send(producerRecord, ((metadata, exception) -> {
                if(exception == null) {
                    log.info(String.format("message sent to partition %, with offset %s",metadata.partition(), metadata.offset()));
                }else {
                    log.error(exception.getMessage());
                    exception.printStackTrace();
                }
            }));
        }
        kafkaProducer.close();
    }

    @Test
    public void test2() {

//        String sendMessage = "test message";
//        ListenableFuture future = kafkaTemplate.send("test", sendMessage);
//        future.addCallback(o -> System.out.println("send message success : " + sendMessage),
//                throwable -> System.out.println("send message fail : " + sendMessage));


    }

}
