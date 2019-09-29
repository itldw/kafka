package itldw;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


public class Producer {


    public static void main(String[] args){

        Properties properties=new Properties();
        properties.put("bootstrap.servers", "192.168.0.120:9092");;
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer=new KafkaProducer<String,String>(properties);

        ProducerRecord<String,String> record=new ProducerRecord<String, String>("CustomerContry","PRECISION PRODUCTS","Flink");
        try {
            //
            //producer.send(record);
            //同步发送
            producer.send(record).get();
            //异步发送
            // producer.send(record,new DemoProducerCallBack());


        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public class DemoProducerCallBack implements Callback{

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e!=null){
                e.printStackTrace();
            }
        }
    }
}
