package br.com.kafkapoc.topicoComTresConsumos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MessageConsumer3 {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        consumer.subscribe(Collections.singletonList("ecommerce.compras"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));

            for (var record: records) {
                System.out.println("Compra nova: ");
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
                System.out.println(record.partition());
            }
        }
    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //Endereço do kafka
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Descerializar  da chave: Pode ser colocado um schema ou outro tipo de classe
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//Descerializar  do valor
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumo-cliente"); //Precisa sempre informar o grupo sempre que consumir algo

        return properties;
    }

}
