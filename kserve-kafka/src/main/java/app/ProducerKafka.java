package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import types.TextTranslation;

public class ProducerKafka {

    public static void main(String[] args) throws URISyntaxException, IOException {



        String topicName = System.getenv().getOrDefault("KAFKA_TOPIC", "to-translate");
        String server = System.getenv().getOrDefault("KAFKA_SERVER", "");


        Properties props = new Properties();

        props.put("bootstrap.servers", server);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "serde.JsonPOJOSerializer");

        Producer<String, TextTranslation> producer = new KafkaProducer<String, TextTranslation>(props);

        String file1 = Files.readString(Paths.get(ProducerKafka.class.getClassLoader()
                .getResource("1.txt")
                .toURI()));

        String file2 = Files.readString(Paths.get(ProducerKafka.class.getClassLoader()
                .getResource("2.txt")
                .toURI()));

        for (int i = 0; i < 10; i++) {

            TextTranslation toTranslate = null;

            if(i % 2 == 0){
                toTranslate = TextTranslation
                        .builder()
                        .textToTranslate(file1)
                        .build();
            }else{
                toTranslate = TextTranslation
                        .builder()
                        .textToTranslate(file2)
                        .build();
            }
            producer.send(new ProducerRecord<>(topicName,
                    Integer.toString(i), toTranslate));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}

