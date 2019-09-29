package itldw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static  void main(String[] args){

        Properties properties=new Properties();
        properties.put("bootstrap.servers", "192.168.0.120:9092");
        properties.put("group.id","ContryCounter");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singleton("CustomerContry"));

        try {

            while (true){
                ConsumerRecords<String,String> record=consumer.poll(100);
                for(ConsumerRecord<String,String> r:record){
                    System.out.println(r.partition()+" "+r.topic()+" "+r.key()+" "+r.value());
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
